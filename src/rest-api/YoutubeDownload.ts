import { HexaBackupStore } from '../HexaBackupStore.js'
import { LoggerBuilder, HashTools } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization.js'
import { spawn } from 'child_process'
import * as fs from 'fs'
import * as fsPath from 'path'
import * as Operations from '../Operations.js'
import * as BackgroundJobs from '../BackgroundJobs.js'
import * as SourceState from '../SourceState.js'

const log = LoggerBuilder.buildLogger('plugins-youtube')

const uuid = () => HashTools.hashStringSync(`${Date.now()}-${Math.random()}-${Math.random()}`)

// yt-dlp working version: https://github.com/yt-dlp/yt-dlp/releases/download/2021.10.22/yt-dlp
const YoutubeDownloadExecutable = 'yt-dlp' // 'youtube-dl'

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
        this.backgroundJobs = backgroundJobs.createClient(YoutubeDownloadExecutable)
    }

    updateYoutubeDl() {
        return new Promise(resolve => {
            const child = spawn(YoutubeDownloadExecutable, ['-U'])

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
                    log.err(`${YoutubeDownloadExecutable} update error code ${code} (${signal})`)
                    resolve(false)
                }
            })
        })
    }

    downloadYoutubeUrl(url: string, directory: string) {
        return new Promise(resolve => {
            log(`downloading in directory ${directory}`)

            let programName = YoutubeDownloadExecutable
            let programArguments = [
                '-x',
                '-i',
                '--rm-cache-dir',
                '--no-progress',
                '--yes-playlist',
                '--split-chapters',
                '-f', 'bestaudio',
                '--audio-format', 'mp3', // best
                '--audio-quality', '0',
                '-o', '%(artist)s-%(title)s.%(ext)s',
                url
            ]

            log(`launching ${programName} ${programArguments.join(' ')}`)

            const ytdlLog = LoggerBuilder.buildLogger(YoutubeDownloadExecutable)

            //const child = spawn(YoutubeDownloadExecutable, ['-x', '-i', '--rm-cache-dir', '--no-progress', '--yes-playlist', '-f', 'bestaudio', '-o', '%(artist)s-%(title)s.%(ext)s', url], {
            const child = spawn(programName, programArguments, {
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
                    ytdlLog.err(`exit error code ${code} (${signal})`)
                    resolve(false)
                }
            })
        })
    }

    async grabFromYoutube(url: string, sourceId: string) {
        log(`fetch youtube from url ${url} on source ${sourceId}`)

        await this.updateYoutubeDl()
        log(`${YoutubeDownloadExecutable} is up to date`)

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
            return { error: `no files after running ${YoutubeDownloadExecutable}` }
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
        try {
            let files = fs.readdirSync(directory)
            files && files.map(name => fsPath.join(directory, name)).forEach(path => fs.unlinkSync(path))
            fs.rmdirSync(directory)
        }
        catch (err) {
            log.wrn(`error removing download dir: ${err}`)
        }

        log(`committing changes`)

        let source = await this.store.getSourceState(sourceId)
        if (!source) {
            source = SourceState.newSourceState()
            SourceState.setIndexed(source, true)
            await this.store.getReferenceRepository().put(sourceId, source)
        }

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
                `${YoutubeDownloadExecutable} ${url}`,
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