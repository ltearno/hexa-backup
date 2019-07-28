import { HexaBackupStore } from '../HexaBackupStore'
import { Queue, LoggerBuilder, OrderedJson, HashTools } from '@ltearno/hexa-js'
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

interface ConversionJob {
    url: string
    sourceId: string
}

export class YoutubeDownload {
    private conversionQueue = new Queue.Queue<ConversionJob>('youtube-conversions')

    constructor(private store: HexaBackupStore) {
    }

    private async conversionLoop() {
        const waiter = Queue.waitPopper(this.conversionQueue)

        while (true) {
            const info = await waiter()
            if (!info) {
                log(`finished conversion loop`)
                break
            }

            try {
                log(`starting youtube conversion ${info.url} on ${info.sourceId}, still ${this.conversionQueue.size()} in queue`)
                let result = await this.grabFromYoutube(info.url, info.sourceId)
                log(`finished conversion (res:${JSON.stringify(result)})`)
            }
            catch (err) {
                log.err(`sorry, failed conversion !`)
                log.err(err)
            }
        }
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
            const child = spawn('youtube-dl', ['-x', '--no-progress', '--yes-playlist', '-f', 'bestaudio', '--audio-format', 'mp3', url], {
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

    async grabFromYoutube(url, sourceId) {
        log(`fetch youtube from url ${url} on source ${sourceId}`)

        await this.updateYoutubeDl()
        log(`youtube-dl is up to date`)

        let tmpDir = fsPath.join(os.homedir(), HashTools.hashStringSync(url + Date.now()))
        fs.mkdirSync(tmpDir)

        await this.downloadYoutubeUrl(url, tmpDir)

        let files = fs.readdirSync(tmpDir)
        if (!files) {
            return { error: `no files after running youtube-dl` }
        }

        let fileNames = files.map(name => fsPath.join(tmpDir, name))
        let contents = []

        for (let fileName of fileNames) {
            let stats = fs.statSync(fileName)
            let contentSha = await HashTools.hashFile(fileName)
            let offset = await this.store.hasOneShaBytes(contentSha)

            const fd = fs.openSync(fileName, 'r')
            if (!fd) {
                log.err(`error reading`)
                fs.closeSync(fd)
                continue
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

            let validated = await this.store.validateShaBytes(contentSha)
            if (validated) {
                contents.push({
                    sha: contentSha,
                    fileName,
                    size: stats.size
                })
            }
            else {
                log.err(`cannot validate downloaded sha`)
            }
        }

        fileNames.forEach(fileName => fs.unlinkSync(fileName))
        fs.rmdirSync(tmpDir)

        // fetch source state
        let sourceState = await this.store.getSourceState(sourceId)
        let commit = sourceState && sourceState.currentCommitSha && await this.store.getCommit(sourceState.currentCommitSha)
        let currentDescriptor = commit && commit.directoryDescriptorSha && await this.store.getDirectoryDescriptor(commit.directoryDescriptorSha)
        if (!currentDescriptor) {
            currentDescriptor = {
                files: []
            }
        }

        // add item
        for (let content of contents) {
            currentDescriptor.files.push({
                contentSha: content.sha,
                name: fsPath.basename(content.fileName),
                isDirectory: false,
                lastWrite: Date.now(),
                size: content.size
            })
        }

        // store new directory descriptor
        let stringified = OrderedJson.stringify(currentDescriptor)
        let descriptorRaw = Buffer.from(stringified, 'utf8')
        let descriptorSha = HashTools.hashStringSync(stringified)

        if (!await this.store.hasOneShaBytes(descriptorSha)) {
            log.dbg(`write directory descriptor after converting ${url} to ${descriptorSha}`)

            await this.store.putShaBytes(descriptorSha, 0, descriptorRaw)
            if (!await this.store.validateShaBytes(descriptorSha)) {
                return { error: `cannot validate directory descriptor` }
            }
        }

        // commit the changes
        let commitSha = await this.store.registerNewCommit(sourceId, descriptorSha)
        log(`went well, commit ${commitSha} with descriptor ${descriptorSha} descriptor ${descriptorSha}`)

        return { ok: `committed with commit ${commitSha}`, convertedFiles: contents }
    }

    addEnpointsToApp(app: any) {
        this.conversionLoop()

        app.post('/plugins/youtube/fetch', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let user = await Authorization.getUserFromRequest(req)
            if (!user) {
                res.send(JSON.stringify({ error: `no user specified` }))
                return
            }

            let request = req.body as YoutubeFetchRequest

            // url sourceId
            let url = request.url
            let sourceId = userSourceId(user)

            this.conversionQueue.push({ url, sourceId })

            res.send(JSON.stringify({ ok: `conversion pushed` }))
        })
    }
}