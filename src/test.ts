import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as FS from 'fs'
import * as Stream from 'stream'
import * as Net from 'net'
import * as Serialization from './serialisation'
import { ShaCache } from './ShaCache'
import { HexaBackupStore } from './HexaBackupStore'

const log = require('./Logger')('Tests');


interface FileInfo {
    name: string;
    isDirectory: boolean;
    lastWrite: number;
    size: number;
}

interface FileAndShaInfo extends FileInfo {
    contentSha: string;
}

class ShaProcessor extends Stream.Transform {
    private shaCache: ShaCache

    constructor() {
        super({ objectMode: true })

        this.shaCache = new ShaCache('d:\\tmp\\tmp\\exp-cache')
    }

    _flush(callback) {
        callback()
    }

    async _transform(chunk: FileInfo, encoding, callback: (err, data) => void) {
        let err = null
        let value = null

        if (chunk.isDirectory) {
            value = Object.assign({ contentSha: null }, chunk)
        }
        else {
            try {
                let sha = await this.shaCache.hashFile(chunk.name)
                value = Object.assign({ contentSha: sha }, chunk)
            } catch (e) {
                log(`ERROR SHAING ${e}`)
                err = e
            }
        }

        callback(err, value)
    }
}

class DirectoryLister extends Stream.Readable {
    private stack: string[]
    private awaitingReaddir: boolean = false

    constructor(private path: string, private ignoredNames: string[]) {
        super({ objectMode: true })

        this.stack = [this.path]
    }

    async _read(size: number) {
        if (this.awaitingReaddir)
            return

        if (this.stack.length > 0) {
            let currentPath = this.stack.pop();

            this.awaitingReaddir = true
            let files = await FsTools.readDir(currentPath);
            this.awaitingReaddir = false

            for (let key in files) {
                let fileName = files[key];
                if (this.ignoredNames.some(name => fileName == name))
                    continue;

                let fullFileName = fsPath.join(currentPath, fileName);
                let stat = fs.statSync(fullFileName);

                let desc: FileInfo = {
                    name: fullFileName,
                    isDirectory: stat.isDirectory(),
                    lastWrite: stat.mtime.getTime(),
                    size: 0
                };

                if (stat.isDirectory())
                    this.stack.push(fullFileName)
                else
                    desc.size = stat.size

                this.push(desc)
            }
        }

        if (!this.awaitingReaddir && this.stack.length == 0) {
            this.push(null)
        }
    }
}


const MSG_TYPE_ASK_SHA_STATUS = 0
const MSG_TYPE_REP_SHA_STATUS = 1
const MSG_TYPE_ADD_SHA_IN_TX = 2
const MSG_TYPE_SHA_BYTES = 3


let port = 5001

let server = Net.createServer((socket) => {
    log('client connected')

    let store = new HexaBackupStore('D:\\tmp\\tmp-store')

    class ShaWriter extends Stream.Writable {
        constructor() {
            super({ objectMode: true })
        }

        async _write(data, encoding, callback) {
            await store.putShaBytes(data.sha, data.offset, data.buffer)
            callback()
        }
    }

    let shaWriter = new ShaWriter()

    socket.on('message', async (message) => {
        let [messageType, param1 = null, param2 = null, param3 = null] = Serialization.deserialize(message, null)

        //log(`SERVER RCV TYPE ${messageType}`)

        switch (messageType) {
            case MSG_TYPE_ASK_SHA_STATUS: {
                let sha = param1
                let reqId = param2
                let size = await store.hasOneShaBytes(sha)
                sendMessageToSocket(Serialization.serialize([MSG_TYPE_REP_SHA_STATUS, [reqId, size]]), socket)
                break
            }

            case MSG_TYPE_ADD_SHA_IN_TX: {
                let fileInfo = param1 as FileAndShaInfo
                break
            }

            case MSG_TYPE_SHA_BYTES: {
                let sha = param1
                let offset = param2
                let buffer = param3

                shaWriter.write({ sha, offset, buffer })
                break
            }

            default:
                log.err(`unknown rx msg type ${messageType}`)
        }
    })

    socketDataToMessage(socket)

    socket.on('close', () => {
        log('connection from client closed')
    })
})

server.on('error', (err) => log.err(`server error: ${err}`))

server.listen(port)




let socket = new Net.Socket()

socket.on('connect', () => {
    log(`connected to ${server}:${port}`)

    initCommunication(socket)
})

socket.connect(port, "localhost")





class AddShaInTxPayloadsStream extends Stream.Transform {
    constructor() {
        super({ objectMode: true })
    }

    _flush(callback) {
        callback()
    }

    async _transform(fileAndShaInfo: FileAndShaInfo, encoding, callback: (err, data) => void) {
        callback(null, Serialization.serialize([MSG_TYPE_ADD_SHA_IN_TX, fileAndShaInfo]))
    }
}

/**
 * Receives a file's raw bytes and send them by block
 */
class ShaBytesPayloadsStream extends Stream.Transform {
    constructor(private clientStatus: ClientStatus, private fileInfo: FileAndShaInfo, private offset: number) {
        super({ objectMode: true })

        let fsAny = fs as any
        let fileBytesStream = fsAny.createReadStream(fileInfo.name, {
            flags: 'r',
            encoding: null,
            start: offset
        })

        fileBytesStream.pipe(this)
    }

    _flush(callback) {
        this.clientStatus.addToTransaction(this.fileInfo)
        callback(null, null)
    }

    async _transform(buffer: Buffer, encoding, callback: (err, data) => void) {
        this.push(Serialization.serialize([MSG_TYPE_SHA_BYTES, this.fileInfo.contentSha, this.offset, buffer]))
        this.offset += buffer.length

        callback(null, null)
    }
}

type ReadableStream = Stream.Readable | Stream.Transform

interface StreamInfo {
    name: string;
    stream: ReadableStream;
}

class ClientStatus {
    streams: StreamInfo[] = []

    addShaInTxPayloadsStream = new AddShaInTxPayloadsStream()

    pendingAskShaStatus: Map<number, FileAndShaInfo> = new Map()
    pendingSendShaBytes: { fileInfo: FileAndShaInfo; offset: number; }[] = []

    private isNetworkDraining: boolean = true

    constructor(private socket: Net.Socket) {
    }

    private addStream(name: string, stream: ReadableStream) {
        let si = {
            name,
            stream
        }

        this.streams.push(si)

        log(`added stream ${si.name}`)

        this.initStream(si)
    }

    private initStream(stream: StreamInfo) {
        stream.stream.on('end', () => {
            this.isNetworkDraining = true

            log(`finished source stream ${stream.name}`)
            this.streams = this.streams.filter(s => s != stream)

            if (this.streams.length == 0)
                log(`FINISHED WORK !`)
            else
                this.maybeSendBytesToNetwork()
        })

        stream.stream.on('readable', () => {
            this.maybeSendBytesToNetwork()
        })
    }

    private maybeSendBytesToNetwork() {
        if (!this.isNetworkDraining)
            return

        //log(`TRY SEND BYTES`)

        for (let i = this.streams.length - 1; i >= 0; i--) {
            let si = this.streams[i]

            while (this.isNetworkDraining) {
                let chunk = si.stream.read(1)
                if (chunk == null) {
                    //log(`NOTHING IN STREAM ${si.name}`)
                    break
                }

                //log(`CHUNK FROM ${si.name}`)

                this.isNetworkDraining = sendMessageToSocket(chunk, this.socket)
            }
        }

        // log(`GONE SENDING BYTES, DRAINING : ${this.isNetworkDraining}`)
    }

    start() {
        let askShaStatusPayloadsStream = new AskShaStatusPayloadsStream(this)
        this.addStream("AskShaStatus", askShaStatusPayloadsStream)
        this.addStream("AddShaInTransaction", this.addShaInTxPayloadsStream)

        let directoryLister = new DirectoryLister('d:\\tmp\\tmp', ['.git', 'exp-cache'])
        let shaProcessor = new ShaProcessor()
        directoryLister
            .pipe(shaProcessor)
            .pipe(askShaStatusPayloadsStream)

        this.socket.on('drain', () => {
            this.isNetworkDraining = true
            this.maybeSendBytesToNetwork()
        })

        let registeredShasForSending = new Set<string>()

        this.socket.on('message', (message) => {
            let [messageType, content] = Serialization.deserialize(message, null)

            switch (messageType) {
                case MSG_TYPE_REP_SHA_STATUS:
                    let reqId = content[0]
                    let size = content[1]

                    let matchedPending = this.pendingAskShaStatus.get(reqId)
                    if (!matchedPending) {
                        log.err(`error, received a non matched SHA size, reqId=${reqId}`)
                        return
                    }

                    this.pendingAskShaStatus.delete(reqId)

                    //log(`received ${sha} remote size, still waiting for ${this.pendingAskShaStatus.length}`)
                    if (matchedPending.size != size && !registeredShasForSending.has(matchedPending.contentSha)) {
                        registeredShasForSending.add(matchedPending.contentSha)

                        let offset = size
                        if (size > matchedPending.size) {
                            log(`warning : remote sha ${matchedPending.contentSha} is bigger than expected, restarting transfer`)
                            offset = 0
                        }

                        this.addStream(`FileTransfert from ${offset} ${matchedPending.name}`, new ShaBytesPayloadsStream(this, matchedPending, offset))
                    }
                    else {
                        this.addToTransaction(matchedPending)
                    }

                    break

                default:
                    log.err(`received unknown msg type ${messageType}`)
            }
        })

        this.socket.on('close', () => {
            log('connection to server closed')
        })

        socketDataToMessage(this.socket)

        this.isNetworkDraining = true
        this.maybeSendBytesToNetwork()
    }

    addToTransaction(fileAndShaInfo: FileAndShaInfo) {
        this.addShaInTxPayloadsStream.write(fileAndShaInfo)
    }

    addPendingAskShaStatus(reqId: number, fileAndShaInfo: FileAndShaInfo) {
        this.pendingAskShaStatus.set(reqId, fileAndShaInfo)
    }
}

class AskShaStatusPayloadsStream extends Stream.Transform {
    private nextAskShaStatusReqId = 1

    constructor(private clientStatus: ClientStatus) {
        super({ objectMode: true })
    }

    _flush(callback) {
        callback()
    }

    async _transform(fileAndShaInfo: FileAndShaInfo, encoding, callback: (err, data) => void) {
        if (fileAndShaInfo.isDirectory)
            this.clientStatus.addToTransaction(fileAndShaInfo)
        else {
            let reqId = this.nextAskShaStatusReqId++
            this.push(Serialization.serialize([MSG_TYPE_ASK_SHA_STATUS, fileAndShaInfo.contentSha, reqId]))
            this.clientStatus.addPendingAskShaStatus(reqId, fileAndShaInfo)
        }

        callback(null, null)
    }
}


function initCommunication(socket: Net.Socket) {
    let clientStatus = new ClientStatus(socket)

    clientStatus.start()
}




function sendMessageToSocket(payload, socket) {
    let header = new Buffer(4)
    header.writeInt32LE(payload.length, 0)
    socket.write(header)
    return socket.write(payload)
}

function socketDataToMessage(socket: Net.Socket) {
    let currentMessage: Buffer = null
    let currentMessageBytesToFill = 0

    let counterBuffer = new Buffer(4)
    let counterBufferOffset = 0

    socket.on('data', (chunk: Buffer) => {
        let offsetInSource = 0

        try {
            while (true) {
                if (currentMessageBytesToFill === 0 && currentMessage) {
                    socket.emit('message', currentMessage)
                    currentMessage = null
                }

                if (offsetInSource >= chunk.length)
                    break

                if (currentMessageBytesToFill === 0) {
                    let counterLength = 4 - counterBufferOffset
                    if (chunk.length - offsetInSource < counterLength)
                        counterLength = chunk.length - offsetInSource

                    chunk.copy(counterBuffer, counterBufferOffset, offsetInSource, offsetInSource + counterLength)
                    counterBufferOffset += counterLength
                    offsetInSource += counterLength

                    if (counterBufferOffset == 4) {
                        // get length
                        currentMessageBytesToFill = counterBuffer.readInt32LE(0)
                        counterBufferOffset = 0

                        // allocate next buffer
                        currentMessage = new Buffer(currentMessageBytesToFill)
                    }

                    continue
                }

                // copy some bytes
                let copyLength = chunk.length - offsetInSource
                if (copyLength > currentMessageBytesToFill)
                    copyLength = currentMessageBytesToFill

                if (copyLength > 0) {
                    let offsetInDest = currentMessage.length - currentMessageBytesToFill
                    chunk.copy(currentMessage, offsetInDest, offsetInSource, offsetInSource + copyLength)
                    currentMessageBytesToFill -= copyLength
                    offsetInSource += copyLength
                }
            }
        }
        catch (e) {
            log.err(`error processing socket incoming data`)
        }
    })
}



