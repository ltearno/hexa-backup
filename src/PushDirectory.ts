import * as Tools from './Tools'
import * as fs from 'fs'
import * as path from 'path'
import * as DirectoryBrowser from './DirectoryBrowser'
import { Readable } from 'stream'
import * as ShaCache from './ShaCache'
import { LoggerBuilder, Queue } from '@ltearno/hexa-js'
import { IHexaBackupStore } from './HexaBackupStore'

interface ShaToSend {
    fileInfo: DirectoryBrowser.Entry
    offset: number
}

const log = LoggerBuilder.buildLogger('ClientPeering')

export class PushDirectory {
    fileInfos = new Queue.Queue<DirectoryBrowser.Entry>('file-entries')
    shasToSend = new Queue.Queue<ShaToSend>('shas-to-send')

    async startPushLoop(pushedDirectory: string, pushDirectories: boolean, store: IHexaBackupStore) {
        let sentBytes = 0
        let sendingTime = 0
        let isBrowsing = false
        let isSending = false
        let isValidating = false
        let sentDirectories = 0
        let sentFiles = 0

        let f2q: FileStreamToQueuePipe = null

        let lastStartSendTime = 0
        let lastShaToSend: ShaToSend = null

        let statusArray = () => {
            let shaStats = shaCache.stats()
            return [
                `queues:               files ${this.fileInfos.size()} / ${isBrowsing ? '[BROWSING]' : '[FINISHED]'} / shasToSend ${this.shasToSend.size()}`,
                `browsing:             ${directoryBrowser.stats.nbDirectoriesBrowsed} dirs, ${directoryBrowser.stats.nbFilesBrowsed} files, ${Tools.prettySize(directoryBrowser.stats.bytesBrowsed)} browsed ${isBrowsing ? '' : ' [FINISHED]'}`,
                `hashing:              ${Tools.prettySize(shaStats.totalHashedBytes)} hashed, ${Tools.prettyTime(shaStats.totalTimeHashing)}, ${Tools.prettySpeed(shaStats.totalHashedBytes, shaStats.totalTimeHashing)}, ${Tools.prettySize(shaStats.totalBytesCacheHit)} cache hit`,
                `tx:                   ${Tools.prettySize(sentBytes)}, ${Tools.prettyTime(sendingTime)}, ${Tools.prettySpeed(sentBytes, sendingTime)}, ${sentDirectories} directories, ${sentFiles} files`
            ]
        }

        log.setStatus(() => {
            let res = statusArray()

            if (isSending) {
                if (lastShaToSend.fileInfo.model.isDirectory) {
                    res = res.concat([``, `                      sending directory`])
                }
                else {
                    let transferred = (f2q ? f2q.transferred : 0)
                    let txTime = Date.now() - lastStartSendTime
                    let leftBytes = (lastShaToSend.fileInfo.model.size - (lastShaToSend.offset + transferred))
                    let eta = transferred > 0 ? (txTime * leftBytes) / transferred : 0
                    res = res.concat([
                        ` sending:             ${lastShaToSend.fileInfo.model.contentSha.substr(0, 7)} ${lastShaToSend.fileInfo.fullPath}`,
                        ` progress:            ${Tools.prettySize(lastShaToSend.offset + transferred)}/${Tools.prettySize(lastShaToSend.fileInfo.model.size)}${lastShaToSend.offset ? `, started at ${Tools.prettySize(lastShaToSend.offset)}` : ''} ${Tools.prettySpeed(transferred, txTime)} ${(100 * (lastShaToSend.offset + transferred) / lastShaToSend.fileInfo.model.size).toFixed(2)} % (${Tools.prettySize(leftBytes)} left, ETA ${Tools.prettyTime(eta)})`
                    ])
                }
            }
            else if (isValidating) {
                res = res.concat([``, `                      (remote validating ${Tools.prettySize(lastShaToSend.fileInfo.model.size)})`])
            }
            else {
                res = res.concat([``, `                      (not sending shaBytes)`])
            }

            return res
        })

        let shaCache = new ShaCache.ShaCache(path.join(pushedDirectory, '.hb-cache'))

        let directoryBrowser = new DirectoryBrowser.DirectoryBrowser(
            pushedDirectory,
            Queue.waitPusher(this.fileInfos, 20, 15),
            shaCache
        )
        let directoryDescriptorSha: string = null

        {
            // browse files
            (async () => {
                isBrowsing = true
                directoryDescriptorSha = await directoryBrowser.start()
                log.dbg(`done directory browsing`)
                isBrowsing = false
            })()
        }

        let countBrowsedEntries = 0

        {
            // see what to send
            (async () => {
                const popper = Queue.waitPopper(this.fileInfos)
                while (true) {
                    const fileInfo = await popper()
                    if (!fileInfo) {
                        log(`finished browsing and processing local files`)
                        this.shasToSend.push(null)
                        break
                    }

                    countBrowsedEntries++

                    let remoteLength = await store.hasOneShaBytes(fileInfo.model.contentSha)

                    this.shasToSend.push({ fileInfo, offset: remoteLength })
                }
            })()
        }

        {
            await (async () => {
                let popper = Queue.waitPopper(this.shasToSend)

                let sentShas = new Set<string>()

                while (true) {
                    let shaToSend = await popper()
                    if (!shaToSend) {
                        log(`no more sha to send`)
                        break
                    }

                    const model = shaToSend.fileInfo.model

                    if (sentShas.has(model.contentSha)) {
                        log.dbg(`sha already sent ${model.contentSha.substr(0, 7)}`)
                        continue
                    }

                    // clear cache when too big
                    if (sentShas.size > 1000)
                        sentShas.clear()

                    sentShas.add(model.contentSha)

                    log.dbg(`shaEntry ${JSON.stringify(shaToSend)}`)

                    if (model.size <= shaToSend.offset) {
                        log.dbg(`already on remote ${model.contentSha.substr(0, 7)}`)
                        continue
                    }

                    if (!pushDirectories && model.isDirectory) {
                        log.dbg(`skipping sending directory`)
                        continue
                    }

                    isSending = true
                    lastShaToSend = shaToSend
                    lastStartSendTime = Date.now()

                    if (model.isDirectory) {
                        log.dbg(`sending directory...`)

                        let buffer = Buffer.from(shaToSend.fileInfo.directoryDescriptorRaw, 'utf8')
                        let start = Date.now()
                        await store.putShaBytes(model.contentSha, 0, buffer)
                        sentDirectories++
                        sendingTime += Date.now() - start
                        sentBytes += buffer.length

                        log.dbg(`sent directory`)
                    }
                    else {
                        log.dbg(`pushing ${model.contentSha.substr(0, 7)} ${shaToSend.fileInfo.fullPath} @ ${shaToSend.offset}/${model.size}`)

                        let start = Date.now()
                        const shaBytes = new Queue.Queue<FileChunk>('sha-bytes')
                        f2q = new FileStreamToQueuePipe(shaToSend.fileInfo.fullPath, model.contentSha, shaToSend.offset, shaBytes, 50, 30)
                        const streamFilePromise = f2q.start()
                        {
                            // send bytes
                            await (async () => {
                                const popper = Queue.waitPopper(shaBytes)
                                while (true) {
                                    const info = await popper()
                                    if (!info) {
                                        log.dbg(`finished sending bytes to remote ${shaToSend.fileInfo.fullPath}`)
                                        break
                                    }

                                    const pushed = await store.putShaBytes(...info)
                                    if (pushed != info[2].length) {
                                        log.err(`pushShaBytes failed, returned ${pushed}, called with ${JSON.stringify(info)}`)
                                        break
                                    }
                                }
                            })()
                        }
                        await streamFilePromise

                        sendingTime += Date.now() - start
                        sentBytes += model.size
                        sentFiles++

                        log.dbg(`finished push ${shaToSend.fileInfo.fullPath} speed = ${Tools.prettySize(sentBytes)} in ${sendingTime} => ${Tools.prettySize((1000 * sentBytes) / (sendingTime))}/s`)
                    }

                    isSending = false

                    isValidating = true
                    let pushResult = await store.validateShaBytes(model.contentSha)
                    if (!pushResult)
                        log.err(`sha not validated by remote ${model.contentSha} ${JSON.stringify(shaToSend)}`)
                    isValidating = false
                }

                log(`finished sending shas`)
            })()
        }

        log(`push finished, browsed:${countBrowsedEntries} sent:${sentFiles}`)

        statusArray().forEach(log)

        return directoryDescriptorSha
    }
}

// sha, offset, data
type FileChunk = [string, number, Buffer]

class FileStreamToQueuePipe {
    private s: Readable
    public transferred = 0

    constructor(
        path: string,
        private sha: string,
        private offset: number,
        private q: Queue.QueueWrite<FileChunk> & Queue.QueueMng,
        high: number = 10,
        low: number = 5) {
        this.s = fs.createReadStream(path, { flags: 'r', autoClose: true, start: offset, encoding: null })

        let paused = false

        // queue has too much items => pause inputs
        q.addLevelListener(high, 1, () => {
            //console.log(`pause inputs`)
            paused = true
            this.s.pause()
        })

        // queue has low items => resume inputs
        q.addLevelListener(low, -1, () => {
            //console.log(`resume reading`)
            if (paused)
                this.s.resume()
        })
    }

    start(): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.s.on('data', chunk => {
                let offset = this.offset
                this.offset += chunk.length
                this.q.push([
                    this.sha,
                    offset,
                    chunk as Buffer
                ])
                this.transferred += chunk.length
            }).on('end', () => {
                resolve(true)
                this.q.push(null)
            }).on('error', (err) => {
                console.log(`stream error ${err}`)
                reject(err)
            })
        })
    }
}
