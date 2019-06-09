import { HexaBackupStore } from './HexaBackupStore'
import { Queue, LoggerBuilder } from '@ltearno/hexa-js'
import * as fs from 'fs'
import * as path from 'path'
import { spawn } from 'child_process'

const log = LoggerBuilder.buildLogger('VideoConverter')

interface VideoConversion {
    sha: string,
    waiters: ((convertedFilePath: string) => void)[],
    result: string
}

export class VideoConverter {
    private videoCacheDir = '.hb-videocache'
    private videoConversions = new Map<string, VideoConversion>()
    private videoConversionQueue = new Queue.Queue<VideoConversion>('video-conversions')

    constructor(private store: HexaBackupStore) {
    }

    init() {
        this.videoConversionLoop()
    }

    createSmallVideo(sha: string): Promise<string> {
        return new Promise(resolve => {
            if (this.videoConversions.has(sha)) {
                log(`waiting for existing conversion ${sha}, ${this.videoConversionQueue.size()} in queue`)
                let info = this.videoConversions.get(sha)

                info.waiters.push(resolve)
                return
            }

            let destFile = path.join(this.videoCacheDir, `svhb-${sha}.mp4`)
            if (fs.existsSync(destFile)) {
                resolve(destFile)
                return
            }

            let info = {
                sha,
                waiters: [resolve],
                result: null
            }

            log(`waiting for conversion ${sha}, ${this.videoConversionQueue.size()} in queue`)

            this.videoConversions.set(sha, info)
            this.videoConversionQueue.push(info)

            return
        })
    }

    private convertVideo(sha: string): Promise<string> {
        return new Promise(resolve => {
            try {
                if (!fs.existsSync(this.videoCacheDir))
                    fs.mkdirSync(this.videoCacheDir)

                let destFile = path.join(this.videoCacheDir, `svhb-${sha}.mp4`)
                if (fs.existsSync(destFile)) {
                    resolve(destFile)
                    return
                }

                let inputFile = this.store.getShaFileName(sha)
                if (!fs.existsSync(inputFile)) {
                    resolve(null)
                    return
                }

                const child = spawn('/snap/bin/ffmpeg', [
                    '-y',
                    '-i',
                    inputFile,
                    '-vf',
                    'scale=w=320:h=-2',
                    destFile
                ])

                child.stdout.on('data', (data) => {
                    log(`${data}`)
                })

                child.stderr.on('data', (data) => {
                    log.err(`${data}`)
                })

                child.on('error', (err) => {
                    log.err(`error on spawned process : ${err}`)
                    resolve(null)
                })

                child.on('exit', (code, signal) => {
                    if (!code) {
                        resolve(destFile)
                    }
                    else {
                        log.err(`ffmpeg error code ${code} (${signal})`)
                        resolve(null)
                    }
                })
            }
            catch (err) {
                log.err(`ffmpeg conversion error ${err}`)
                resolve(null)
            }
        })
    }

    private async videoConversionLoop() {
        const waiter = Queue.waitPopper(this.videoConversionQueue)

        while (true) {
            const info = await waiter()
            if (!info) {
                log(`finished video conversion loop`)
                break
            }
            try {
                log(`starting video conversion ${info.sha}, still ${this.videoConversionQueue.size()} in queue`)
                info.result = await this.convertVideo(info.sha)
                log(`finished video conversion ${info.sha}, still ${this.videoConversionQueue.size()} in queue`)
                info.waiters.forEach(w => w(info.result))
            }
            catch (err) {
                log.err(`sorry, failed video conversion !`)
                log.err(err)
                info.waiters.forEach(w => w(null))
            }

            this.videoConversions.delete(info.sha)
        }
    }
}