import { Readable } from 'stream'
import * as ShaCache from './ShaCache'
import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
import { HashTools, FsTools, LoggerBuilder, ExpressTools, Queue, Transport, NetworkApiNodeImpl, NetworkApi, DirectoryLister, StreamToQueue, Tools } from '@ltearno/hexa-js'
import * as Model from './Model'
import * as fs from 'fs'
import * as path from 'path'
import * as DirectoryBrowser from './DirectoryBrowser'

const log = LoggerBuilder.buildLogger('Commands')


enum RequestType {
    ShaBytes = 1,
    Call = 2,
    HasShaBytes = 3
}

type HasShaBytes = [RequestType.HasShaBytes, string] // type, sha
type ShaBytes = [RequestType.ShaBytes, string, number, Buffer] // type, sha, offset, buffer
type RpcCall = [RequestType.Call, string, ...any[]]
type RpcQuery = ShaBytes | RpcCall | HasShaBytes
type RpcReply = any[]

function prettySize(size: number): string {
    if (size < 1024)
        return size.toString()
    if (size < 1024 * 1024)
        return (size / 1024).toFixed(2) + ' kB'
    if (size < 1024 * 1024 * 1024)
        return (size / (1024 * 1024)).toFixed(2) + ' MB'
    return (size / (1024 * 1024 * 1024)).toFixed(2) + ' GB'
}

async function multiInOneOutLoop(sourceQueues: { queue: Queue.Queue<RpcQuery>; listener: (q: RpcQuery) => void }[], rpcTxPusher: Queue.Pusher<RpcQuery>) {
    let waitForQueue = async <T>(q: Queue.Queue<T>): Promise<void> => {
        if (q.empty()) {
            await new Promise(resolve => {
                let l = q.addLevelListener(1, 1, () => {
                    l.forget()
                    resolve()
                })
            })
        }
    }

    while (sourceQueues.length) {
        if (sourceQueues.every(source => source.queue.empty()))
            await Promise.race(sourceQueues.map(source => waitForQueue(source.queue)))

        let rpcRequest = null
        for (let i = 0; i < sourceQueues.length; i++) {
            if (!sourceQueues[i].queue.empty()) {
                rpcRequest = sourceQueues[i].queue.pop()
                sourceQueues[i].listener && sourceQueues[i].listener(rpcRequest)
                if (rpcRequest) {
                    await rpcTxPusher(rpcRequest)
                }
                else {
                    log(`finished rpc source ${sourceQueues[i].queue.name}`)
                    sourceQueues.splice(i, 1)
                }
                break
            }
        }
    }

    log(`finished rpcPush`)
}


function connectToRemoteSocket(host: string, port: number): Promise<NetworkApi.WebSocket> {
    return new Promise((resolve, reject) => {
        let network = new NetworkApiNodeImpl.NetworkApiNodeImpl()
        let ws = network.createClientWebSocket(`ws://${host}:${port}/hexa-backup`)
        let opened = false

        ws.on('open', () => {
            opened = true
            resolve(ws)
        })

        ws.on('error', err => {
            if (!opened)
                reject(err)
        })
    })
}


class Peering {
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

    private rpcResolvers = new Map<RpcCall, (value: any) => void>()
    private rpcRejecters = new Map<RpcCall, (value: any) => void>()

    async start() {
        let transport = new Transport.Transport(
            Queue.waitPopper(this.rpcTxIn),
            Queue.directPusher(this.rpcTxOut),
            Queue.directPusher(this.rpcRxOut),
            Queue.waitPopper(this.rpcRxIn),
            this.ws
        )
        transport.start()

        await this.startRpcLoops()
    }

    private async startRpcLoops() {
        {
            let rpcTxPusher = Queue.waitPusher(this.rpcTxIn, 20, 10)

            let rpcQueues = [{ queue: this.rpcCalls, listener: null }]
            if (this.withPush) {
                rpcQueues = (rpcQueues as any[]).concat([
                    { queue: this.shaBytes, listener: q => null },
                    {
                        queue: this.hasShaBytes, listener: q => {
                            if (q)
                                this.nbHasShaBytesInTransport++
                            else
                                this.closedHasShaBytes = true
                        }
                    }
                ])
            }
            else {
                this.closedHasShaBytes = true
            }

            multiInOneOutLoop(rpcQueues, rpcTxPusher).then(_ => rpcTxPusher(null))
        }

        let popper = Queue.waitPopper(this.rpcTxOut)

        let shasToSendPusher = Queue.waitPusher(this.shasToSend, 20, 10)

        while (true) {
            let rpcItem = await popper()
            if (!rpcItem)
                break

            let { request, reply } = rpcItem
            switch (request[0]) {
                case RequestType.HasShaBytes: {
                    let remoteLength = reply[0]
                    await shasToSendPusher({ sha: request[1], offset: remoteLength })

                    this.nbHasShaBytesInTransport--
                    log.dbg(`rcv hasshabytes reply, ${this.nbHasShaBytesInTransport}, ${this.closedHasShaBytes}`)
                    if (!this.nbHasShaBytesInTransport && this.closedHasShaBytes) {
                        log.dbg(`RECEIVED LAST hasshabytes REPLY, no more shas to send`)
                        await shasToSendPusher(null)
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

        log(`finished rpcTxOut`)
    }

    private callRpc(rpcCall: RpcCall): Promise<any> {
        return new Promise((resolve, reject) => {
            this.rpcResolvers.set(rpcCall, resolve)
            this.rpcRejecters.set(rpcCall, reject)
            this.rpcCalls.push(rpcCall)
        })
    }

    private createProxy<T>(): T {
        let me = this
        return <T>new Proxy({}, {
            get(target, propKey, receiver) {
                return (...args) => {
                    args = args.slice()
                    args.unshift(propKey)
                    args.unshift(RequestType.Call)

                    return me.callRpc(args as RpcCall)
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

                if (sentShas.has(shaToSend.sha)) {
                    log(`sha already sent ${shaToSend.sha}`)
                    continue
                }
                sentShas.add(shaToSend.sha)

                let shaEntry = directoryBrowser.closeEntry(shaToSend.sha)
                if (shaEntry.size <= shaToSend.offset) {
                    log(`already on remote ${shaToSend.sha} ${JSON.stringify(shaEntry)}`)
                    continue
                }

                if (shaEntry.isDirectory) {
                    log(`sending directory...`)
                    let shaBytesPusher = Queue.waitPusher(this.shaBytes, 50, 40)
                    await shaBytesPusher([RequestType.ShaBytes, shaToSend.sha, 0, Buffer.from(shaEntry.descriptorRaw, 'utf8')])
                    log(`sent directory`)
                }
                else {
                    let fileEntry = shaEntry as DirectoryBrowser.OpenedFileEntry

                    log(`begin push ${shaToSend.sha} ${fileEntry.fullPath} @ ${shaToSend.offset}/${fileEntry.size}`)
                    let start = Date.now()

                    let interval = setInterval(() => {
                        log(` ... transferring ${fileEntry.fullPath} (${f2q.transferred / (1024 * 1024)} Mb so far)...`)
                    }, 1000)

                    let f2q = new FileStreamToQueuePipe(fileEntry.fullPath, shaToSend.sha, shaToSend.offset, this.shaBytes, 50, 40)
                    await f2q.start()

                    sendingTime += Date.now() - start
                    sentBytes += fileEntry.size

                    clearInterval(interval)

                    log(`finished push ${fileEntry.fullPath} speed = ${sentBytes} bytes in ${sendingTime} => ${((1000 * sentBytes) / (1024 * 1024 * sendingTime))} Mb/s`)
                }

                // little hooky way of sending a RPC through an arbitrary queue, this is because otherwise the validation could happen before the transfert
                let validateCall = [RequestType.Call, 'validateShaBytes', shaToSend.sha] as RpcQuery
                this.shaBytes.push(validateCall as ShaBytes)
                this.rpcResolvers.set(validateCall as RpcCall, result => {
                    if (!result)
                        log.err(`sha not validated by remote ${shaToSend.sha} ${JSON.stringify(shaEntry)}`)
                })
            }
        })()

        log(`finished shasToSend`)
        this.shasToSend.push(null)

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


export async function refs(storeIp, storePort, verbose) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    console.log(`refs on store`)
    console.log()

    let refs = await store.getRefs()

    if (refs == null) {
        console.log(`refs not found !`)
        return
    }

    for (let ref of refs) {
        console.log(`${ref}`)
    }
}


export async function sources(storeIp, storePort, verbose) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    console.log(`sources on store`)

    let sources = await store.getSources()

    if (sources == null) {
        console.log()
        console.log(`refs not found !`)
        return
    }

    for (let sourceId of sources) {
        console.log()
        console.log(`${sourceId}`)
        try {
            let state = await store.getSourceState(sourceId)
            state.currentTransactionId && console.log(` current transaction : ${state.currentTransactionId}`)
            if (state.currentCommitSha) {
                console.log(` current commit sha : ${state.currentCommitSha}`)
                let commitSha = state.currentCommitSha

                let currentCommit = await store.getCommit(commitSha)
                if (!currentCommit) {
                    console.log(`  commit ${commitSha} not found !`)
                }
                else {
                    let currentDirectoryDescriptor = await store.getDirectoryDescriptor(currentCommit.directoryDescriptorSha)
                    if (!currentDirectoryDescriptor) {
                        console.log(`  descriptor ${currentCommit.directoryDescriptorSha} not found !`)
                    }
                    else {
                        let payload = JSON.stringify(currentDirectoryDescriptor)

                        console.log(` nb descriptor items : ${currentDirectoryDescriptor.files.length}`)
                        console.log(` descriptor size : ${prettySize(payload.length)}`)

                        console.log(` commit history :`)
                        while (commitSha != null) {
                            let commit = await store.getCommit(commitSha)
                            if (commit == null) {
                                console.log(`  error : commit ${commitSha} not found !`)
                                break
                            }

                            console.log(`  ${new Date(commit.commitDate).toDateString()} commit ${commitSha} desc ${commit.directoryDescriptorSha}`)

                            commitSha = commit.parentSha
                        }
                    }
                }
            }
        }
        catch (err) {
            console.log(` error ! ${JSON.stringify(err)}`)
        }
    }
}


export async function history(sourceId, storeIp, storePort, verbose) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    console.log(`history of ${sourceId} in store`);
    console.log()

    let sourceState = await store.getSourceState(sourceId);

    if (sourceState == null) {
        console.log(`source state not found !`)
        return
    }

    if (sourceState.currentTransactionId) {
        console.log()
        console.log(`current transaction ${sourceState.currentTransactionId}`)
    }

    let directoryDescriptorShaToShow = null
    let commitSha = sourceState.currentCommitSha
    if (commitSha == null)
        console.log('empty !')

    while (commitSha != null) {
        let commit = await store.getCommit(commitSha);
        if (commit == null) {
            console.log(`error : commit ${commitSha} not found !`);
            break;
        }

        console.log(`${new Date(commit.commitDate).toDateString()} commit ${commitSha} desc ${commit.directoryDescriptorSha}`);

        if (directoryDescriptorShaToShow == null)
            directoryDescriptorShaToShow = commit.directoryDescriptorSha

        commitSha = commit.parentSha
    }

    if (verbose && directoryDescriptorShaToShow) {
        console.log()
        console.log(`most recent commit's directory structure (${directoryDescriptorShaToShow}) :`)
        let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorShaToShow)
        await showDirectoryDescriptor(directoryDescriptor, store)
    }
}

export async function showCurrentTransaction(sourceId, storeIp, storePort, prefix) {
}

export async function showCommit(storeIp, storePort, commitSha) {
}

export async function lsDirectoryStructure(storeIp, storePort, directoryDescriptorSha, prefix: string) {
    log('connecting to remote store...')

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorSha);

    await showDirectoryDescriptor(directoryDescriptor, store, prefix)
}

export async function extract(storeIp, storePort, directoryDescriptorSha, prefix: string, destinationDirectory: string) {
    log('connecting to remote store...')

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    console.log('getting directory descriptor...')
    let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorSha);

    await showDirectoryDescriptor(directoryDescriptor, store, prefix)

    destinationDirectory = path.resolve(destinationDirectory)

    console.log(`extracting ${directoryDescriptorSha} to ${destinationDirectory}, prefix='${prefix}'...`)

    for (let k in directoryDescriptor.files) {
        let fileDesc = directoryDescriptor.files[k]

        if (prefix && !fileDesc.name.startsWith(prefix))
            continue

        console.log(`fetching ${fileDesc.name}`)

        let destinationFilePath = path.join(destinationDirectory, fileDesc.name)

        if (fileDesc.isDirectory) {
            try {
                fs.mkdirSync(destinationFilePath)
            } catch (error) {
                log("error : " + error)
            }
        }
        else {
            let fileLength = await store.hasOneShaBytes(fileDesc.contentSha)

            let currentReadPosition = 0
            try {
                let stat = await FsTools.lstat(destinationFilePath)
                currentReadPosition = stat.size
            }
            catch (error) {
            }

            let fd = await FsTools.openFile(destinationFilePath, 'a')

            const maxSize = 1024 * 100
            while (currentReadPosition < fileLength) {
                let size = fileLength - currentReadPosition
                if (size > maxSize)
                    size = maxSize

                let buffer = await store.readShaBytes(fileDesc.contentSha, currentReadPosition, size)

                await FsTools.writeFileBuffer(fd, currentReadPosition, buffer)

                currentReadPosition += size
            }

            await FsTools.closeFile(fd)

            let contentSha = await HashTools.hashFile(destinationFilePath)
            if (contentSha != fileDesc.contentSha) {
                log.err(`extracted file signature is inconsistent : ${contentSha} != ${fileDesc.contentSha}`)
            }

            log(`extracted ${fileDesc.name}`)
        }

        let lastWriteUnix = parseInt((fileDesc.lastWrite / 1000).toFixed(0))
        fs.utimesSync(destinationFilePath, lastWriteUnix, lastWriteUnix)
    }
}

export async function push(sourceId, pushedDirectory, storeIp, storePort, estimateSize) {
    log('connecting to remote store...')
    log(`push options :`)
    log(`  directory: ${pushedDirectory}`)
    log(`  source: ${sourceId}`)
    log(`  server: ${storeIp}:${storePort}`)
    log(`  estimateSize: ${estimateSize}`)

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, true)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    log(`starting push`)

    let directoryDescriptorSha = await peering.startPushLoop(pushedDirectory)
    log(`directoryDescriptorSha: ${directoryDescriptorSha}`)

    let commitSha = await store.registerNewCommit(sourceId, directoryDescriptorSha)

    log(`finished push, commit ${commitSha}`)
}

export async function store(directory, port) {
    console.log(`preparing store in ${directory}`);
    let store = new HexaBackupStore(directory);

    console.log('server intialisation')

    let app = ExpressTools.createExpressApp(port)
    app.ws('/hexa-backup', async (ws, req) => {
        console.log(`serving new client ws`)

        let rpcTxIn = new Queue.Queue<RpcQuery>('rpc-tx-in')
        let rpcTxOut = new Queue.Queue<{ request: RpcQuery; reply: RpcReply }>('rpc-tx-out')
        let rpcRxOut = new Queue.Queue<{ id: string; request: RpcQuery }>('rpc-rx-out')
        let rpcRxIn = new Queue.Queue<{ id: string; reply: RpcReply }>('rpc-rx-in')

        let transport = new Transport.Transport(Queue.waitPopper(rpcTxIn), Queue.directPusher(rpcTxOut), Queue.directPusher(rpcRxOut), Queue.waitPopper(rpcRxIn), ws)
        transport.start()

        ws.on('error', err => {
            console.log(`error on ws ${err}`)
            ws.close()
        })

        ws.on('close', () => {
            console.log(`closed ws`)
            rpcRxOut.push(null)
        })

        await Queue.tunnelTransform(
            Queue.waitPopper(rpcRxOut),
            Queue.directPusher(rpcRxIn),
            async (p: { id: string; request: RpcQuery }) => {
                let { id, request } = p

                switch (request[0]) {
                    case RequestType.HasShaBytes:
                        return {
                            id,
                            reply: [await store.hasOneShaBytes(request[1])]
                        }

                    case RequestType.ShaBytes:
                        return {
                            id,
                            reply: [await store.putShaBytes(request[1], request[2], request[3])]
                        }

                    case RequestType.Call:
                        request.shift()
                        let methodName = request.shift()
                        let args = request

                        let method = store[methodName]
                        if (!method) {
                            console.log(`not found method ${methodName} in store !`)
                        }
                        try {
                            let result = await method.apply(store, args)

                            return {
                                id,
                                reply: [result]
                            }
                        }
                        catch (error) {
                            return {
                                id,
                                reply: [null, error]
                            }
                        }
                }
            })

        console.log(`bye bye client ws !`)
    })

    //let transferServer = new UploadTransferServer.UploadTransferServer()
    //transferServer.listen(port + 1, store)

    console.log(`ready on port ${port} !`);
}

export async function browse(directory: string) {
    let queue = new Queue.Queue<Model.FileDescriptor>('filesanddirs')
    let shaCache = new ShaCache.ShaCache('.hb-cache')
    let browser = new DirectoryBrowser.DirectoryBrowser(directory, Queue.waitPusher(queue, 10, 5), shaCache)

    {
        (async () => {
            let popper = Queue.waitPopper(queue)

            while (true) {
                let item = await popper()
                if (!item)
                    break

                let entry = await browser.closeEntry(item.contentSha)

                console.log(`${JSON.stringify(item)}`)
                if (entry.isDirectory) {
                    console.log(`${entry.descriptorRaw}`)
                }
                else {
                    console.log(`${(entry as any).fullPath}`)
                }
            }
        })()
    }

    let wholeSha = await browser.start()
    console.log(`finished, whole sha is ${wholeSha}`)
}

async function showDirectoryDescriptor(directoryDescriptor: Model.DirectoryDescriptor, store: IHexaBackupStore, prefix?: string) {
    let totalSize = 0;
    let nbFiles = 0;
    let nbDirectories = 0;
    directoryDescriptor.files.forEach((fd) => {
        totalSize += fd.size;
        if (fd.isDirectory)
            nbDirectories++;
        else
            nbFiles++;
    });

    console.log(`${totalSize} bytes in ${nbFiles} files, ${nbDirectories} dirs`);

    let emptySha = '                                                                '

    for (let fd of directoryDescriptor.files) {
        if (!prefix || fd.name.startsWith(prefix)) {
            console.log(`${fd.isDirectory ? '<dir>' : '     '} ${new Date(fd.lastWrite).toDateString()} ${('            '
                + (fd.isDirectory ? '' : fd.size)).slice(-12)}  ${fd.contentSha}  ${fd.contentSha ? fd.contentSha : emptySha} ${fd.name} `);

            if (fd.isDirectory && fd.contentSha) {
                let desc = await store.getDirectoryDescriptor(fd.contentSha)
                await showDirectoryDescriptor(desc, store, prefix)
            }
        }
    }
}