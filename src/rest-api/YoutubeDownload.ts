import { HexaBackupStore } from '../HexaBackupStore'
import { LoggerBuilder, OrderedJson, HashTools } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization'
import * as Model from '../Model'
import { spawn } from 'child_process'
import * as fs from 'fs'
import * as os from 'os'
import * as fsPath from 'path'
import { rejects } from 'assert';

const log = LoggerBuilder.buildLogger('plugins-youtube')

/*
youtube-dl -U

youtube-dl -x --audio-format mp3 "https://www.youtube.com/watch?v=H3v9unphfi0"

// system default tmp dir
os.tmpdir()
*/

interface YoutubeFetchRequest {
    url: string
}

function userSourceId(user: string) {
    return `plugin-youtubedownload-${user}`
}


export class YoutubeDownload {
    constructor(private store: HexaBackupStore) {
    }

    updateYoutubeDl() {
        return new Promise((resolve, reject) => {
            const child = spawn('youtube-dl', ['-U'])

            child.stdout.on('data', (data) => {
                log(`${data}`)
            })

            child.stderr.on('data', (data) => {
                log.err(`${data}`)
            })

            child.on('error', (err) => {
                log.err(`error on spawned process : ${err}`)
            })

            child.on('exit', (code, signal) => {
                if (!code) {
                    resolve()
                }
                else {
                    log.err(`youtube-dl update error code ${code} (${signal})`)
                    reject()
                }
            })
        })
    }

    downloadYoutubeUrl(url: string, directory: string) {
        return new Promise((resolve, reject) => {
            log(`downloading in directory ${directory}`)
            const child = spawn('youtube-dl', ['-x', '-f', 'bestaudio', '--audio-format', 'mp3', url], {
                cwd: directory
            })

            child.stdout.on('data', (data) => {
                log(`${data}`)
            })

            child.stderr.on('data', (data) => {
                log.err(`${data}`)
            })

            child.on('error', (err) => {
                log.err(`error on spawned process : ${err}`)
            })

            child.on('exit', (code, signal) => {
                if (!code) {
                    log('done')
                    resolve()
                }
                else {
                    log.err(`youtube-dl error code ${code} (${signal})`)
                    reject()
                }
            })
        })
    }

    addEnpointsToApp(app: any) {
        app.post('/plugins/youtube/fetch', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let user = await Authorization.getUserFromRequest(req)
            if (!user) {
                res.send(JSON.stringify({ error: `no user specified` }))
                return
            }

            let request = req.body as YoutubeFetchRequest
            log(`fetch youtube from url ${request.url}`)

            await this.updateYoutubeDl()
            log(`youtube-dl is up to date`)

            let tmpDir = fsPath.join(os.homedir(), HashTools.hashStringSync(request.url + Date.now()))
            fs.mkdirSync(tmpDir)

            await this.downloadYoutubeUrl(request.url, tmpDir)

            let files = fs.readdirSync(tmpDir)
            if (!files || files.length != 1) {
                res.send(JSON.stringify({ result: 'error' }))
                return
            }

            let fileName = fsPath.join(tmpDir, files[0])

            let stats = fs.statSync(fileName)
            let contentSha = await HashTools.hashFile(fileName)
            let offset = await this.store.hasOneShaBytes(contentSha)

            const fd = fs.openSync(fileName, 'r')
            if (!fd) {
                log.err(`error reading`)
                return
            }

            while (offset < stats.size) {
                const length = Math.min(1024 * 1024, stats.size - offset)
                let buffer = Buffer.alloc(length)

                let nbRead = fs.readSync(fd, buffer, 0, length, offset)
                if (nbRead != length) {
                    log.err(`inconsistent read`)
                    break
                }

                await this.store.putShaBytes(contentSha, offset, buffer)

                offset += length
            }

            fs.closeSync(fd)

            fs.unlinkSync(fileName)
            fs.rmdirSync(tmpDir)

            let validated = await this.store.validateShaBytes(contentSha)
            if (!validated) {
                log.err(`cannot validate downloaded sha`)
                res.send(JSON.stringify({ result: 'cannot validate downloaded sha' }))
                return
            }

            // fetch source state
            let sourceId = userSourceId(user)
            let sourceState = await this.store.getSourceState(sourceId)
            let commit = sourceState && sourceState.currentCommitSha && await this.store.getCommit(sourceState.currentCommitSha)
            let currentDescriptor = commit && commit.directoryDescriptorSha && await this.store.getDirectoryDescriptor(commit.directoryDescriptorSha)
            if (!currentDescriptor) {
                currentDescriptor = {
                    files: []
                }
            }

            // add item
            currentDescriptor.files.push({
                contentSha,
                name: fsPath.basename(fileName),
                isDirectory: false,
                lastWrite: Date.now(),
                size: stats.size
            })

            // store new directory descriptor
            let stringified = OrderedJson.stringify(currentDescriptor)
            let descriptorRaw = Buffer.from(stringified, 'utf8')
            let descriptorSha = HashTools.hashStringSync(stringified)

            if (!await this.store.hasOneShaBytes(descriptorSha)) {
                log.dbg(`write directory descriptor for user ${user}, name ${fsPath.basename(fileName)} to ${descriptorSha}`)

                await this.store.putShaBytes(descriptorSha, 0, descriptorRaw)
                if (!await this.store.validateShaBytes(descriptorSha)) {
                    res.send(JSON.stringify({ error: `cannot validate directory descriptor content` }))
                    return
                }
            }

            // commit the changes
            let commitSha = await this.store.registerNewCommit(sourceId, descriptorSha)
            res.send(JSON.stringify({ ok: `went well, commit ${commitSha} with descriptor ${descriptorSha} descriptor ${descriptorSha}` }))
        })
    }
}