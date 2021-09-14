import { HexaBackupStore } from '../HexaBackupStore'
import { LoggerBuilder, HashTools } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization'
import { spawn } from 'child_process'
import * as fs from 'fs'
import * as fsPath from 'path'
import * as Operations from '../Operations'
import * as BackgroundJobs from '../BackgroundJobs'

const log = LoggerBuilder.buildLogger('plugins-youtube')

const uuid = () => HashTools.hashStringSync(`${Date.now()}-${Math.random()}-${Math.random()}`)

interface YoutubeFetchRequest {
    url: string
}

function userSourceId(user: string) {
    return `plugin-youtubedownload-${user}`
}

export class YoutubeDownload {
    private conversionCacheDir = '.hb-youtubedlcache'
    private backgroundJobs: BackgroundJobs.BackgroundJobClientApi

    constructor(private store: HexaBackupStore, backgroundJobs: BackgroundJobs.BackgroundJobs) {
        this.backgroundJobs = backgroundJobs.createClient(`youtube-dl`)
    }

    updateYoutubeDl() {
        return new Promise(resolve => {
            const child = spawn('youtube-dl', ['-U'])

            child.stdout.on('data', (data) => {
                log(`${data}`.trim())
            })

            child.stderr.on('data', (data) => {
                log.err(`${data}`.trim())
            })

            child.on('error', (err) => {
                log.err(`error on spawned process : ${err}`)
            })

            child.on('exit', (code, signal) => {
                if (!code) {
                    resolve(true)
                }
                else {
                    log.err(`youtube-dl update error code ${code} (${signal})`)
                    resolve(false)
                }
            })
        })
    }

    downloadYoutubeUrl(url: string, directory: string) {
        return new Promise(resolve => {
            log(`downloading in directory ${directory}`)

            const ytdlLog = LoggerBuilder.buildLogger('youtube-dl')

            //const child = spawn('youtube-dl', ['-x', '-i', '--rm-cache-dir', '--no-progress', '--yes-playlist', '-f', 'bestaudio', '-o', '%(artist)s-%(title)s.%(ext)s', url], {
            const child = spawn('youtube-dl', ['-x', '-i', '--rm-cache-dir', '--no-progress', '--yes-playlist', '-f', 'bestaudio', '--audio-format', 'best', '--audio-quality', '0', '-o', '%(artist)s-%(title)s.%(ext)s', url], {
                cwd: directory
            })

            child.stdout.on('data', (data) => {
                ytdlLog(`${data}`.trim())
            })

            child.stderr.on('data', (data) => {
                ytdlLog.err(`${data}`.trim())
            })

            child.on('error', (err) => {
                ytdlLog.err(`error on spawned process : ${err}`)
            })

            child.on('exit', (code, signal) => {
                if (!code) {
                    ytdlLog('done')
                    resolve(true)
                }
                else {
                    ytdlLog.err(`youtube-dl error code ${code} (${signal})`)
                    resolve(false)
                }
            })
        })
    }

    async grabFromYoutube(url: string, sourceId: string) {
        log(`fetch youtube from url ${url} on source ${sourceId}`)

        await this.updateYoutubeDl()
        log(`youtube-dl is up to date`)

        const directory = `${this.conversionCacheDir}/${uuid()}`
        fs.mkdirSync(directory, { recursive: true })

        try {
            await this.downloadYoutubeUrl(url, directory)
        }
        catch (error) {
            log.err(`download from youtube failed`)
            return { error: `download from youtube failed` }
        }

        let files = fs.readdirSync(directory)
        if (!files) {
            log.err(`no files downloaded`)
            return { error: `no files after running youtube-dl` }
        }

        let fileNames = files.map(name => fsPath.join(directory, name))
        let contents: { sha: string; path: string; size: number; }[] = []

        log(`files downloaded, now pushing ${fileNames.length} files to repo`)

        for (let fileName of fileNames) {
            if (fileName.endsWith('.part'))
                continue

            log(`pushing ${fileName} to repo`)
            let pushed = await Operations.pushLocalFileToStore(fileName, this.store)
            if (pushed)
                contents.push(pushed)
            else
                log.err(`failed to push ${fileName} to repository !`)
        }

        log(`removing download directory`)
        fs.rmSync(directory, { force: true, recursive: true })

        log(`committing changes`)
        // fetch source current state
        let currentDescriptor = await Operations.getSourceCurrentDirectoryDescriptor(sourceId, this.store)
        if (!currentDescriptor) {
            currentDescriptor = {
                files: []
            }
        }

        // add items
        let lastWrite = Date.now()
        for (let content of contents) {
            if (content.path.endsWith('.part'))
                continue

            currentDescriptor.files.push({
                contentSha: content.sha,
                name: fsPath.basename(content.path),
                isDirectory: false,
                lastWrite,
                size: content.size
            })
        }

        // optimize the descriptor (remove duplicates)
        let oldFiles = currentDescriptor.files
        let seenStrings = new Set<string>()
        currentDescriptor.files = []
        for (let i = oldFiles.length - 1; i >= 0; i--) {
            let oldFile = oldFiles[i]

            if (seenStrings.has(oldFile.contentSha) || seenStrings.has(oldFile.name)) {
                log(`removing ${oldFile.contentSha}: ${oldFile.name} because it is duplicate in the youtube dl playlist`)
                continue
            }

            seenStrings.add(oldFile.contentSha)
            seenStrings.add(oldFile.name)

            currentDescriptor.files.unshift(oldFile)
        }

        let commitSha = await Operations.commitDirectoryDescriptor(sourceId, currentDescriptor, this.store)
        if (!commitSha)
            return { error: `failed to commit directory descriptor` }

        log(`went well, commit ${commitSha}`)

        return { ok: `committed with commit ${commitSha}`, convertedFiles: contents }
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

            let url = request.url
            let sourceId = userSourceId(user)

            this.backgroundJobs.addJob(
                `youtube-dl ${url}`,
                async () => {
                    try {
                        log(`starting youtube conversion ${url} on ${sourceId}`)
                        let result = await this.grabFromYoutube(url, sourceId)
                        log(`finished conversion (res:${JSON.stringify(result)})`)
                        return result
                    }
                    catch (err) {
                        log.err(`sorry, failed conversion !`)
                        log.err(err)
                        return null
                    }
                })

            res.send(JSON.stringify({ ok: `conversion pushed` }))
        })
    }
}