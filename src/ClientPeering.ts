import * as Tools from './Tools'
import { Readable } from 'stream'
import * as ShaCache from './ShaCache'
import { IHexaBackupStore } from './HexaBackupStore'
import { LoggerBuilder, Queue, Transport, NetworkApi } from '@ltearno/hexa-js'
import * as Model from './Model'
import * as fs from 'fs'
import * as path from 'path'
import * as DirectoryBrowser from './DirectoryBrowser'
import {
    RequestType,
    HasShaBytes,
    RpcCall,
    RpcQuery,
    RpcReply,
    ShaBytes
} from './RPC'

const log = LoggerBuilder.buildLogger('ClientPeering')

/**
 * This is not a good architecture (duplication with store server code, and it is not yet modular)
 * 
 * But this splits Commands.ts into two files, which is more practical...
 */

export class Peering {
    constructor(private ws: NetworkApi.WebSocket, private withPush: boolean) { }

    rpcCalls = new Queue.Queue<RpcCall>('rpc-calls')

    fileInfos = new Queue.Queue<Model.FileDescriptor>('file-entries')
    hasShaBytes = new Queue.Queue<HasShaBytes>('has-sha-bytes')
    closedHasShaBytes = false
    nbHasShaBytesInTransport = 0
    shasToSend = new Queue.Queue<{ sha: string; offset: number }>('shas-to-send')
    shaBytes = new Queue.Queue<ShaBytes>('sha-bytes')

    remoteStore = this.createProxy<IHexaBackupStore>()

    private rpcTxIn = new Queue.Queue<RpcQuery>('rpc-tx-in')
    private rpcTxOut = new Queue.Queue<{ request: RpcQuery; reply: RpcReply }>('rpc-tx-out')
    private rpcRxIn = new Queue.Queue<{ id: string; reply: RpcReply }>('rpc-rx-in')
    private rpcRxOut = new Queue.Queue<{ id: string; request: RpcQuery }>('rpc-rx-out')

    private rpcResolvers = new Map<RpcQuery, (value: any) => void>()
    private rpcRejecters = new Map<RpcQuery, (value: any) => void>()

    async start() {
        let transport = new Transport.Transport(
            Queue.waitPopper(this.rpcTxIn),
            Queue.directPusher(this.rpcTxOut),
            Queue.directPusher(this.rpcRxOut),
            Queue.waitPopper(this.rpcRxIn),
            this.ws
        )
        transport.start()

        this.ws.on('close', () => {
            this.rpcRxOut.push(null)
        })

        await this.startRpcLoops()
    }

    private async startRpcLoops() {
        {
            let rpcTxPusher = Queue.waitPusher(this.rpcTxIn, 20, 10)

            let rpcQueues: any[] = [{ queue: this.rpcCalls, listener: null }]
            if (this.withPush) {
                rpcQueues = [
                    { queue: this.shaBytes, listener: () => null },
                    { queue: this.rpcCalls, listener: null },
                    {
                        queue: this.hasShaBytes, listener: q => {
                            if (q)
                                this.nbHasShaBytesInTransport++
                            else {
                                this.closedHasShaBytes = true
                                log.dbg(`hasShaBytes is now closed`)
                            }
                        }
                    }
                ]
            }
            else {
                this.closedHasShaBytes = true
            }

            Queue.manyToOneTransfert(rpcQueues, rpcTxPusher).then(_ => rpcTxPusher(null))
        }

        let popper = Queue.waitPopper(this.rpcTxOut)

        while (true) {
            let rpcItem = await popper()
            if (!rpcItem)
                break

            let { request, reply } = rpcItem
            switch (request[0]) {
                case RequestType.HasShaBytes: {
                    log.dbg(`rcv hasshabytes reply, ${this.nbHasShaBytesInTransport}, ${this.closedHasShaBytes}`)

                    let remoteLength = reply[0]
                    this.shasToSend.push({ sha: request[1], offset: remoteLength })

                    this.nbHasShaBytesInTransport--
                    if (!this.nbHasShaBytesInTransport && this.closedHasShaBytes) {
                        log.dbg(`received last HasShaBytes reply`)
                        this.shasToSend.push(null)
                    }

                    break
                }

                case RequestType.ShaBytes:
                    break

                case RequestType.Call:
                    if (reply.length == 1) {
                        log.dbg(`rcv reply ${JSON.stringify(reply)} for request ${JSON.stringify(request)}`)
                        this.rpcResolvers.get(request)(reply[0])
                    }
                    else if (reply.length > 1) {
                        log(`exception received as a result ${JSON.stringify(reply[1])} call ${JSON.stringify(request)}`)
                        this.rpcRejecters.get(request)(reply[1])
                    }
                    this.rpcResolvers.delete(request)
                    this.rpcRejecters.delete(request)
                    break
            }
        }

        log(`finished rpc replies queue`)
    }

    private async callRpcOn(rpcCall: RpcCall, queue: Queue.Queue<RpcQuery>): Promise<any> {
        await Queue.waitAndPush(queue, rpcCall, 10, 8)
        //queue.push(rpcCall)

        let result = new Promise((resolve, reject) => {
            this.rpcResolvers.set(rpcCall, resolve)
            this.rpcRejecters.set(rpcCall, reject)
        })

        return result
    }

    private createProxy<T>(): T {
        let me = this
        return <T>new Proxy({}, {
            get(_, propKey) {
                return (...args) => {
                    args = args.slice()
                    args.unshift(propKey)
                    args.unshift(RequestType.Call)

                    return me.callRpcOn(args as RpcCall, me.rpcCalls)
                };
            }
        });
    }

    status = {
        hashedBytes: 0
    }

    async startPushLoop(pushedDirectory: string) {
        let shaCache = new ShaCache.ShaCache(path.join(pushedDirectory, '.hb-cache'))
        let directoryBrowser = new DirectoryBrowser.DirectoryBrowser(
            pushedDirectory,
            Queue.waitPusher(this.fileInfos, 20, 15),
            shaCache
        )
        let directoryDescriptorSha: string = null

        {
            (async () => {
                directoryDescriptorSha = await directoryBrowser.start()
                this.fileInfos.push(null)
                log(`done directory browsing`)
            })()
        }

        Queue.tunnelTransform(
            Queue.waitPopper(this.fileInfos),
            Queue.waitPusher(this.hasShaBytes, 20, 15),
            async i => {
                return [
                    RequestType.HasShaBytes,
                    i.contentSha
                ] as HasShaBytes
            }
        ).then(_ => {
            log(`finished directory parsing`)
            this.hasShaBytes.push(null)
        })

        await (async () => {
            let popper = Queue.waitPopper(this.shasToSend)

            let sentShas = new Set<string>()

            let sentBytes = 0
            let sendingTime = 0

            while (true) {
                let shaToSend = await popper()
                if (!shaToSend)
                    break

                let shaEntry = directoryBrowser.closeEntry(shaToSend.sha)

                if (sentShas.has(shaToSend.sha)) {
                    log.dbg(`sha already sent ${shaToSend.sha.substr(0, 7)}`)
                    continue
                }
                sentShas.add(shaToSend.sha)

                if (!shaEntry) {
                    log.wrn(`void entry in shasToSend ${shaToSend.sha}`)
                    continue
                }

                log.dbg(`shaEntry ${JSON.stringify(shaEntry)}`)

                if (shaEntry.size <= shaToSend.offset) {
                    log.dbg(`already on remote ${shaToSend.sha.substr(0, 7)}`)
                    continue
                }

                if (shaEntry.isDirectory) {
                    log.dbg(`sending directory...`)
                    let shaBytesPusher = Queue.waitPusher(this.shaBytes, 50, 40)
                    await shaBytesPusher([RequestType.ShaBytes, shaToSend.sha, 0, Buffer.from(shaEntry.descriptorRaw, 'utf8')])
                    log.dbg(`sent directory`)
                }
                else {
                    let fileEntry = shaEntry as DirectoryBrowser.OpenedFileEntry

                    log.dbg(`pushing ${shaToSend.sha.substr(0, 7)} ${fileEntry.fullPath} @ ${shaToSend.offset}/${fileEntry.size}`)
                    let start = Date.now()

                    let interval = setInterval(() => {
                        log(` ... transferring ${fileEntry.fullPath} (${Tools.prettySize(f2q.transferred)}/${Tools.prettySize(fileEntry.size)} so far, ${((1000 * f2q.transferred) / (1024 * 1024 * (Date.now() - start))).toFixed(2)} Mb/s)...`)
                    }, 1000)

                    let f2q = new FileStreamToQueuePipe(fileEntry.fullPath, shaToSend.sha, shaToSend.offset, this.shaBytes, 50, 40)
                    await f2q.start()

                    sendingTime += Date.now() - start
                    sentBytes += fileEntry.size

                    clearInterval(interval)

                    log(`finished push ${fileEntry.fullPath} speed = ${Tools.prettySize(sentBytes)} in ${sendingTime} => ${((1000 * sentBytes) / (1024 * 1024 * sendingTime)).toFixed(2)} Mb/s`)
                }

                // for validation not to happen before sha sending
                // TODO think better about interlocking.
                // If we await on the promise, the response might be stuck in the rxout queue, not processed because waiting for shasToSend queue to empty
                let pushResult = await this.callRpcOn([RequestType.Call, 'validateShaBytes', shaToSend.sha], this.shaBytes)
                if (!pushResult)
                    log.err(`sha not validated by remote ${shaToSend.sha} ${JSON.stringify(shaEntry)}`)
            }
        })()

        log(`finished sending shas`)
        this.shaBytes.push(null)

        return directoryDescriptorSha
    }
}


class FileStreamToQueuePipe {
    private s: Readable
    public transferred = 0

    constructor(path: string, private sha: string, private offset: number, private q: Queue.QueueWrite<ShaBytes> & Queue.QueueMng, high: number = 10, low: number = 5) {
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
                    RequestType.ShaBytes,
                    this.sha,
                    offset,
                    chunk as Buffer
                ])
                this.transferred += chunk.length
            }).on('end', () => {
                resolve(true)
            }).on('error', (err) => {
                console.log(`stream error ${err}`)
                reject(err)
            })
        })
    }
}
