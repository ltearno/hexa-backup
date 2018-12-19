import * as ShaProcessor from './sha-processor'
import * as ShaCache from './ShaCache'
import * as Net from 'net'
import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
import { HashTools, FsTools, LoggerBuilder, ExpressTools, Queue, Transport, NetworkApiNodeImpl, NetworkApi, DirectoryLister, StreamToQueue, Tools } from '@ltearno/hexa-js'
import * as Model from './Model'
import * as fs from 'fs'
import * as path from 'path'

const log = LoggerBuilder.buildLogger('Commands')


enum RequestType {
    AddShaInTx = 0,
    ShaBytes = 1,
    Call = 2
}

interface FileSpec {
    name: string
    isDirectory: boolean
    lastWrite: number
    size: number
}

type AddShaInTx = [RequestType.AddShaInTx, string, string, FileSpec] // type,tx, sha, file
type AddShaInTxReply = [number] // length
type ShaBytes = [RequestType.ShaBytes, string, number, Buffer] // type, sha, offset, buffer
type RpcCall = [RequestType.Call, string, ...any[]]
type RpcQuery = AddShaInTx | ShaBytes | RpcCall
type RpcReply = any[]



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
    constructor(private ws: NetworkApi.WebSocket, private withPushFast: boolean) { }

    rpcCalls = new Queue.Queue<RpcCall>('rpc-calls')

    fileInfos = new Queue.Queue<DirectoryLister.FileIteration>('fileslist')
    addShaInTx = new Queue.Queue<AddShaInTx>('add-sha-in-tx')
    closedAddShaInTx = false
    nbAddShaInTxInTransport = 0
    shasToSend = new Queue.Queue<{ sha: string; file: FileSpec; offset: number }>('shas-to-send')
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
            if (this.withPushFast) {
                rpcQueues = (rpcQueues as any[]).concat([
                    { queue: this.shaBytes, listener: q => null },
                    {
                        queue: this.addShaInTx, listener: q => {
                            if (q)
                                this.nbAddShaInTxInTransport++
                            else
                                this.closedAddShaInTx = true
                        }
                    }
                ])
            }
            else {
                this.closedAddShaInTx = true
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
                case RequestType.AddShaInTx: {
                    let remoteLength = (reply as AddShaInTxReply).length
                    if (!request[3].isDirectory && remoteLength < request[3].size) {
                        await shasToSendPusher({ sha: request[2], file: request[3], offset: remoteLength })
                    }

                    this.nbAddShaInTxInTransport--
                    if (!this.nbAddShaInTxInTransport && this.closedAddShaInTx)
                        await shasToSendPusher(null)

                    break
                }

                case RequestType.ShaBytes:
                    break

                case RequestType.Call:
                    if (reply.length == 1) {
                        log(`rcv reply ${JSON.stringify(reply)} for request ${JSON.stringify(request)}`)
                        this.rpcResolvers.get(request)(reply[0])
                    }
                    else if (reply.length > 1) {
                        log(`exception received as a result ${JSON.stringify(reply[1])}`)
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

    startPushFastLoop(transactionId: string, pushedDirectory: string) {
        let shaCache = new ShaCache.ShaCache(path.join(pushedDirectory, '.hb-cache'))

        // sending files should be done in push fast
        {
            let directoryLister = new DirectoryLister.DirectoryLister('./', () => null);
            (async () => {
                let s2q1 = new StreamToQueue.StreamToQueuePipe(directoryLister, this.fileInfos, 50, 10)
                await s2q1.start()
                this.fileInfos.push(null)
            })()
        }

        Queue.tunnelTransform(
            Queue.waitPopper(this.fileInfos),
            Queue.waitPusher(this.addShaInTx, 50, 8),
            async i => {
                return [
                    RequestType.AddShaInTx,
                    transactionId,
                    i.isDirectory ? '' : await shaCache.hashFile(path.join(pushedDirectory, i.name)),
                    i
                ] as AddShaInTx
            }
        ).then(_ => {
            log(`finished directory parsing`)
            this.addShaInTx.push(null)
        })
    }
}


export async function history(sourceId, storeIp, storePort, verbose) {
    log('connecting to remote store...')

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    console.log('history of pc-arnaud in store');
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

        console.log(`${new Date(commit.commitDate).toDateString()} commit ${commitSha} desc:${commit.directoryDescriptorSha}`);

        if (directoryDescriptorShaToShow == null)
            directoryDescriptorShaToShow = commit.directoryDescriptorSha

        commitSha = commit.parentSha
    }

    if (verbose && directoryDescriptorShaToShow) {
        console.log()
        console.log(`most recent commit's directory structure (${directoryDescriptorShaToShow}) :`)
        let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorShaToShow)
        showDirectoryDescriptor(directoryDescriptor)
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

    showDirectoryDescriptor(directoryDescriptor, prefix)
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

    showDirectoryDescriptor(directoryDescriptor, prefix)

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

export async function pushFast(sourceId, pushedDirectory, storeIp, storePort, estimateSize) {
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

    let txId = await store.startOrContinueSnapshotTransaction(sourceId)
    log(`starting transaction ${txId}`)

    peering.startPushFastLoop(txId, pushedDirectory)

    await Tools.wait(5000)
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
        let rpcRxIn = new Queue.Queue<{ id: string; reply: RpcReply }>('rpc-rx-in')
        let rpcRxOut = new Queue.Queue<{ id: string; request: RpcQuery }>('rpc-rx-out')

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
                    case RequestType.AddShaInTx:
                        await store.pushFileDescriptors(request[1], [{
                            name: request[3].name,
                            isDirectory: request[3].isDirectory,
                            lastWrite: request[3].lastWrite,
                            size: request[3].size,
                            contentSha: request[2]
                        }])

                        let knownBytes = await store.hasOneShaBytes(request[2])
                        return {
                            id,
                            reply: knownBytes
                        }

                    case RequestType.ShaBytes:
                        return {
                            id,
                            reply: await store.putShaBytes(request[1], request[2], request[3])
                        }

                    case RequestType.Call:
                        request.shift()
                        let methodName = request.shift()
                        let args = request

                        let method = store[methodName]
                        if (!method) {
                            console.log(`not found method ${methodName} in store !`)
                        }
                        let result = await method.apply(store, args)

                        return {
                            id,
                            reply: result
                        }
                }
            })

        console.log(`bye bye client ws !`)
    })

    //let transferServer = new UploadTransferServer.UploadTransferServer()
    //transferServer.listen(port + 1, store)

    console.log(`ready on port ${port} !`);
}

function showDirectoryDescriptor(directoryDescriptor: Model.DirectoryDescriptor, prefix?: string) {
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

    directoryDescriptor.files.forEach((fd) => {
        if (!prefix || fd.name.startsWith(prefix))
            console.log(`${fd.isDirectory ? '<dir>' : '     '} ${new Date(fd.lastWrite).toDateString()} ${('            ' + (fd.isDirectory ? '' : fd.size)).slice(-12)}    ${fd.contentSha ? fd.contentSha : emptySha} ${fd.name} `);
    });
}