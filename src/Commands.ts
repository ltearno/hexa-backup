import { Readable } from 'stream'
import * as ShaCache from './ShaCache'
import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
import { HashTools, FsTools, LoggerBuilder, ExpressTools, Queue, Transport, NetworkApiNodeImpl, NetworkApi, OrderedJson } from '@ltearno/hexa-js'
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

const log = LoggerBuilder.buildLogger('Commands')

const DATE_DISPLAY_OPTIONS = {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
}

const displayDate = (date: number | Date) => (typeof date === 'number' ? new Date(date) : date).toLocaleString('fr', DATE_DISPLAY_OPTIONS)

interface TreeDirectoryInfo {
    files: Model.FileDescriptor[]
    name: string
    lastWrite: number
    directories: TreeDirectoryInfo[]
}

function prettySize(size: number): string {
    if (size < 1024)
        return size.toString() + ' bytes'
    if (size < 1024 * 1024)
        return (size / 1024).toFixed(2) + ' kB'
    if (size < 1024 * 1024 * 1024)
        return (size / (1024 * 1024)).toFixed(3) + ' MB'
    return (size / (1024 * 1024 * 1024)).toFixed(3) + ' GB'
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
            else {
                log.err(`websocket error: ${err}`)
                ws.close()
            }
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

        this.ws.on('close', err => {
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
                    { queue: this.shaBytes, listener: q => null },
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
            get(target, propKey, receiver) {
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
                        log(` ... transferring ${fileEntry.fullPath} (${prettySize(f2q.transferred)}/${prettySize(fileEntry.size)} so far, ${((1000 * f2q.transferred) / (1024 * 1024 * (Date.now() - start))).toFixed(2)} Mb/s)...`)
                    }, 1000)

                    let f2q = new FileStreamToQueuePipe(fileEntry.fullPath, shaToSend.sha, shaToSend.offset, this.shaBytes, 50, 40)
                    await f2q.start()

                    sendingTime += Date.now() - start
                    sentBytes += fileEntry.size

                    clearInterval(interval)

                    log(`finished push ${fileEntry.fullPath} speed = ${prettySize(sentBytes)} in ${sendingTime} => ${((1000 * sentBytes) / (1024 * 1024 * sendingTime)).toFixed(2)} Mb/s`)
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
                console.log(` current commit sha : ${state.currentCommitSha.substr(0, 7)}`)
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
                        console.log(` descriptor size     : ${prettySize(payload.length)}`)

                        console.log(` commit history      :`)
                        while (commitSha != null) {
                            let commit = await store.getCommit(commitSha)
                            if (commit == null) {
                                console.log(`  error : commit ${commitSha} not found !`)
                                break
                            }

                            console.log(`  ${displayDate(commit.commitDate)} commit ${commitSha.substr(0, 7)} desc ${commit.directoryDescriptorSha.substr(0, 7)}`)

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


export async function history(sourceId: string, storeIp: string, storePort: number, verbose: boolean) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    console.log(`history of ${sourceId} in store`)
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
        let commit = await store.getCommit(commitSha)
        if (commit == null) {
            console.log(`error : commit ${commitSha} not found !`)
            break
        }

        console.log(`${displayDate(commit.commitDate)}`)
        console.log(` commit: ${commitSha}`)
        console.log(` desc:   ${commit.directoryDescriptorSha}`)
        console.log('')

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

async function loadTreeDirectoryInfoFromDirectoryDescriptor(store: IHexaBackupStore, directoryDescriptor: Model.DirectoryDescriptor): Promise<TreeDirectoryInfo> {
    let rootDirectory: TreeDirectoryInfo = {
        name: '',
        lastWrite: 0,
        files: [],
        directories: []
    }

    let getDirectory = (parts: string[]) => {
        let cur = rootDirectory

        for (let i = 0; i < parts.length; i++) {
            let child = cur.directories.find(c => c.name == parts[i])
            if (!child) {
                child = {
                    name: parts[i],
                    lastWrite: 0,
                    directories: [],
                    files: []
                }
                cur.directories.push(child)
            }

            cur = child
        }

        return cur
    }

    let dirAndBase = (p: string): [string[], string] => {
        let parts = p.split(/[\/\\]/)
        let base = parts.pop()
        return [parts, base]
    }

    directoryDescriptor.files.forEach(d => {
        let [dir, base] = dirAndBase(d.name)
        let baseDirectory = getDirectory(dir)

        if (d.isDirectory) {
            if (!d.contentSha || d.contentSha.trim() == '') {
                let directory = getDirectory(d.name.split(/[\/\\]/))
                directory.lastWrite = d.lastWrite
            }
            else {
                // copy existing
                baseDirectory.files.push({
                    name: base,
                    contentSha: d.contentSha,
                    isDirectory: true,
                    lastWrite: d.lastWrite,
                    size: d.size
                })
            }
        }
        else {
            // copy existing
            baseDirectory.files.push({
                name: base,
                contentSha: d.contentSha,
                isDirectory: false,
                lastWrite: d.lastWrite,
                size: d.size
            })
        }
    })

    return rootDirectory
}

export async function normalize(sourceId: string, storeIp: string, storePort: number, verbose: boolean) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    log(`normalize source ${sourceId}`)

    let state = await store.getSourceState(sourceId)
    if (!state) {
        log.err(`cannot get remote source state`)
        return
    }

    if (!state.currentCommitSha) {
        log.err(`remote has no commit (${sourceId})`)
        return
    }

    let currentCommit = await store.getCommit(state.currentCommitSha)
    if (!currentCommit) {
        log.err(`cannot get remote commit ${state.currentCommitSha}`)
        return
    }

    let startSha = currentCommit.directoryDescriptorSha

    let directoryDescriptor = await store.getDirectoryDescriptor(startSha)
    log(`actual descriptor: ${startSha} ${directoryDescriptor.files.length} files`)

    let rootDirectory = await loadTreeDirectoryInfoFromDirectoryDescriptor(store, directoryDescriptor)

    let shasToSend = new Map<string, Buffer>()

    // browse the tree structure and register each new directory
    let hier2Flat = (d: TreeDirectoryInfo): Model.DirectoryDescriptor => {
        let out: Model.DirectoryDescriptor = {
            files: []
        }

        d.directories.forEach(subDir => {
            let subDirDescriptor = hier2Flat(subDir)
            let stringified = OrderedJson.stringify(subDirDescriptor)
            let subDirDescriptorRaw = Buffer.from(stringified, 'utf8')
            let subDirDescriptorSha = HashTools.hashStringSync(stringified)

            shasToSend.set(subDirDescriptorSha, subDirDescriptorRaw)

            out.files.push({
                name: subDir.name,
                isDirectory: true,
                lastWrite: subDir.lastWrite,
                size: subDirDescriptorRaw.length,
                contentSha: subDirDescriptorSha
            })
        })

        out.files = out.files.concat(d.files)

        return out
    }

    let rootDescriptor = hier2Flat(rootDirectory)
    let stringified = OrderedJson.stringify(rootDescriptor)
    let rootDescriptorRaw = Buffer.from(stringified, 'utf8')
    let rootDescriptorSha = HashTools.hashStringSync(stringified)
    shasToSend.set(rootDescriptorSha, rootDescriptorRaw)
    log(`   new descriptor: ${rootDescriptorSha} ${rootDescriptor.files.length} files`)

    let i = 0
    for (let [sha, content] of shasToSend) {
        i++
        let len = await store.hasOneShaBytes(sha)
        if (len != content.length) {
            log(`send directory ${i}/${shasToSend.size} ${sha}`)
            await store.putShaBytes(sha, 0, content)
            let ok = await store.validateShaBytes(sha)
            if (!ok)
                log.err(`sha not validated ${sha}`)
        }
        else {
            log(`directory already in store ${i}/${shasToSend.size} ${sha}`)
        }
    }

    let result = await store.registerNewCommit(sourceId, rootDescriptorSha)
    log(`finished normalization: ${result}`)
}

export async function lsDirectoryStructure(storeIp: string, storePort: number, directoryDescriptorSha: string, recursive: boolean) {
    log('connecting to remote store...')

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    if (directoryDescriptorSha.length != 64) {
        let autocompleted = await store.autoCompleteSha(directoryDescriptorSha)
        if (!autocompleted) {
            log.err(`cannot find autocompletion for ${directoryDescriptorSha}`)
            return
        }
        log(` sha autocompleted: ${directoryDescriptorSha} => ${autocompleted}`)
        directoryDescriptorSha = autocompleted
    }

    let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorSha)

    await showDirectoryDescriptor(directoryDescriptor, store, directoryDescriptorSha.substr(0, 7), recursive)
}

export async function extract(storeIp: string, storePort: number, directoryDescriptorSha: string, prefix: string, destinationDirectory: string) {
    log('connecting to remote store...')

    let shaCache = new ShaCache.ShaCache('.hb-cache')

    let ws = await connectToRemoteSocket(storeIp, storePort)
    log('connected')

    let peering = new Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    if (directoryDescriptorSha.length != 64) {
        let autocompleted = await store.autoCompleteSha(directoryDescriptorSha)
        if (!autocompleted) {
            log.err(`cannot find autocompletion for ${directoryDescriptorSha}`)
            return
        }
        log(` sha autocompleted: ${directoryDescriptorSha} => ${autocompleted}`)
        directoryDescriptorSha = autocompleted
    }

    destinationDirectory = path.resolve(destinationDirectory)

    console.log(`extracting ${directoryDescriptorSha} to ${destinationDirectory}, prefix='${prefix}'...`)
    await extractDirectoryDescriptor(store, shaCache, directoryDescriptorSha, prefix, destinationDirectory)
}

async function extractDirectoryDescriptor(store: IHexaBackupStore, shaCache: ShaCache.ShaCache, directoryDescriptorSha: string, prefix: string, destinationDirectory: string) {
    console.log('getting directory descriptor...')
    let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorSha)

    await showDirectoryDescriptorSummary(directoryDescriptor)

    for (let k in directoryDescriptor.files) {
        let fileDesc = directoryDescriptor.files[k]

        if (prefix && !fileDesc.name.startsWith(prefix))
            continue

        console.log(`fetching ${fileDesc.name} ${prettySize(fileDesc.size)}`)
        let currentReadPosition = 0

        let displayResume = false
        let timer = setInterval(() => {
            displayResume = true
            console.log(` ${prettySize(currentReadPosition)}`)
        }, 1000)

        let destinationFilePath = path.join(destinationDirectory, fileDesc.name)

        if (fileDesc.isDirectory) {
            try {
                if (!await FsTools.fileExists(destinationFilePath))
                    fs.mkdirSync(destinationFilePath)

                if (fileDesc.contentSha) {
                    await extractDirectoryDescriptor(store, shaCache, fileDesc.contentSha, '', destinationFilePath)
                }
            } catch (error) {
                log("error : " + error)
            }
        }
        else {
            let fileLength = await store.hasOneShaBytes(fileDesc.contentSha)

            try {
                let stat = await FsTools.lstat(destinationFilePath)
                currentReadPosition = stat.size
            }
            catch (error) {
            }

            if (!(await FsTools.fileExists(destinationFilePath)) || fileDesc.contentSha != await shaCache.hashFile(destinationFilePath)) {
                let fd = await FsTools.openFile(destinationFilePath, 'a')

                const maxSize = 1024 * 100
                let writePromise = null
                while (currentReadPosition < fileLength) {
                    let size = fileLength - currentReadPosition
                    if (size > maxSize)
                        size = maxSize

                    let buffer = await store.readShaBytes(fileDesc.contentSha, currentReadPosition, size)

                    if (writePromise)
                        await writePromise
                    writePromise = FsTools.writeFileBuffer(fd, currentReadPosition, buffer)

                    currentReadPosition += size
                }

                await FsTools.closeFile(fd)
            }
            else {
                log.dbg(`already extracted ${destinationFilePath}`)
            }

            let contentSha = await HashTools.hashFile(destinationFilePath)
            if (contentSha != fileDesc.contentSha) {
                log.err(`extracted file signature is inconsistent : ${contentSha} != ${fileDesc.contentSha}`)
            }
        }

        if (displayResume)
            console.log(`extracted ${fileDesc.name}`)
        clearInterval(timer)

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
    log(`directory descriptor  : ${directoryDescriptorSha}`)

    let commitSha = await store.registerNewCommit(sourceId, directoryDescriptorSha)

    log(`finished push, commit : ${commitSha}`)
}

export async function store(directory: string, port: number) {
    console.log(`preparing store in ${directory}`);
    let store = new HexaBackupStore(directory);

    console.log('server intialisation')

    let app = ExpressTools.createExpressApp(port)
    app.ws('/hexa-backup', async (ws: NetworkApi.WebSocket, req: any) => {
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
                        let count = await store.hasOneShaBytes(request[1])
                        return {
                            id,
                            reply: [count]
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
                            log.wrn(`error doing RPC call ${error} ${method}(${JSON.stringify(args)})`)
                            return {
                                id,
                                reply: [null, error]
                            }
                        }
                }
            })

        console.log(`bye bye client ws !`)
    })

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

async function showDirectoryDescriptorSummary(directoryDescriptor: Model.DirectoryDescriptor) {
    let totalSize = 0;
    let nbFiles = 0;
    let nbDirectories = 0;
    directoryDescriptor.files.forEach((fd) => {
        totalSize += fd.size
        if (fd.isDirectory)
            nbDirectories++
        else
            nbFiles++
    });

    console.log(`total ${prettySize(totalSize)} in ${nbFiles} files, ${nbDirectories} dirs`)
}

async function showDirectoryDescriptor(directoryDescriptor: Model.DirectoryDescriptor, store: IHexaBackupStore, currentPath: string = '.', recursive: boolean = false) {
    console.log(``)
    console.log(`${currentPath}:`)
    showDirectoryDescriptorSummary(directoryDescriptor)

    for (let fd of directoryDescriptor.files) {
        let lastWrite = new Date(fd.lastWrite)

        console.log(`${displayDate(lastWrite)} ${fd.contentSha ? fd.contentSha.substr(0, 5) : '   -   '} ${prettySize(fd.size).padStart(12)}   ${fd.name}${fd.isDirectory ? '/' : ''}`)
    }

    if (recursive) {
        for (let fd of directoryDescriptor.files) {
            if (fd.isDirectory && fd.contentSha) {
                let desc = await store.getDirectoryDescriptor(fd.contentSha)
                await showDirectoryDescriptor(desc, store, path.join(currentPath, fd.name), recursive)
            }
        }
    }
}