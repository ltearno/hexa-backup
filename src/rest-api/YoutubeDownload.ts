import { HexaBackupStore } from '../HexaBackupStore'
import { Queue, LoggerBuilder } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization'
import { spawn } from 'child_process'
import * as fs from 'fs'
import * as fsPath from 'path'
import * as Operations from '../Operations'

const log = LoggerBuilder.buildLogger('plugins-youtube')

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
    private conversionCacheDir = '.hb-youtubedlcache'
    private conversionQueue = new Queue.Queue<ConversionJob>('youtube-conversions')

    constructor(private store: HexaBackupStore) { }

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
        return new Promise((resolve, reject) => {
            log(`downloading in directory ${directory}`)
            const child = spawn('youtube-dl', ['-x', '-i', '--no-progress', '--yes-playlist', '-f', 'bestaudio', url], {
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
                    resolve(true)
                }
                else {
                    log.err(`youtube-dl error code ${code} (${signal})`)
                    resolve(false)
                }
            })
        })
    }

    async grabFromYoutube(url, sourceId) {
        log(`fetch youtube from url ${url} on source ${sourceId}`)

        await this.updateYoutubeDl()
        log(`youtube-dl is up to date`)

        if (!fs.existsSync(this.conversionCacheDir))
            fs.mkdirSync(this.conversionCacheDir)
        const directory = this.conversionCacheDir

        await this.downloadYoutubeUrl(url, directory)

        let files = fs.readdirSync(directory)
        if (!files) {
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

        log(`committing changes`)

        fileNames.forEach(fileName => fs.unlinkSync(fileName))

        // fetch source current state
        let currentDescriptor = await Operations.getSourceCurrentDirectoryDescriptor(sourceId, this.store)
        if (!currentDescriptor) {
            currentDescriptor = {
                files: []
            }
        }

        // add item
        for (let content of contents) {
            if (content.path.endsWith('.part'))
                continue

            if (currentDescriptor.files.some(file => file.contentSha == content.sha)) {
                log(`already uploaded ${content.sha} (${content.path}), skipped`)
                continue
            }

            currentDescriptor.files.push({
                contentSha: content.sha,
                name: fsPath.basename(content.path),
                isDirectory: false,
                lastWrite: Date.now(),
                size: content.size
            })
        }

        let commitSha = await Operations.commitDirectoryDescriptor(sourceId, currentDescriptor, this.store)
        if (!commitSha)
            return { error: `failed to commit directory descriptor` }

        log(`went well, commit ${commitSha}`)

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