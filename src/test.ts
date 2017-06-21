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

        log(`ShaProcessor::emit ${value} ${err}`)
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

                log(`DirectoryLister::emit ${desc.name}`)
                this.push(desc)
            }
        }

        if (!this.awaitingReaddir && this.stack.length == 0) {
            log(`DirectoryLister::end`)
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

    socket.on('message', async (message) => {
        let [messageType, content] = Serialization.deserialize(message, null)

        log(`RECEIV DEOM CLIENT`)

        switch (messageType) {
            case MSG_TYPE_ASK_SHA_STATUS:
                let sha = content
                let size = await store.hasOneShaBytes(sha)
                sendMessageToSocket(Serialization.serialize([MSG_TYPE_REP_SHA_STATUS, [sha, size]]), socket)
                break

            case MSG_TYPE_ADD_SHA_IN_TX:
                let fileInfo = content as FileAndShaInfo
                log(`RCV ADD SHA IN TX ${JSON.stringify(fileInfo)}`)
                break

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
        this.push(Serialization.serialize([MSG_TYPE_ADD_SHA_IN_TX, fileAndShaInfo]))
    }
}

/**
 * Receives a file's raw bytes and send them by block
 */
class ShaBytesPayloadsStream extends Stream.Transform {
    constructor(private fileInfo: FileAndShaInfo, private offset: number) {
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
        callback()
    }

    async _transform(buffer: Buffer, encoding, callback: (err, data) => void) {
        this.push(Serialization.serialize([MSG_TYPE_SHA_BYTES, this.fileInfo.contentSha, this.offset, buffer]))
        this.offset + buffer.length
    }
}

type ReadableStream = Stream.Readable | Stream.Transform

interface StreamInfo {
    name: string;
    stream: ReadableStream;
    readable: boolean;
}

class ClientStatus {
    streams: StreamInfo[] = []

    addShaInTxPayloadsStream = new AddShaInTxPayloadsStream()

    pendingAskShaStatus: FileAndShaInfo[] = []
    pendingSendShaBytes: { fileInfo: FileAndShaInfo; offset: number; }[] = []

    constructor(private socket: Net.Socket) {
    }

    private addStream(name: string, stream: ReadableStream) {
        let si = {
            name,
            stream,
            readable: false
        }

        this.streams.push(si)

        this.initStream(si)
    }

    private pauseStreams() {
        for (let stream of this.streams)
            stream.stream.pause()
    }

    private resumeStreams() {
        for (let stream of this.streams) {
            stream.stream.resume()
        }
    }

    private initStream(stream: StreamInfo) {
        stream.stream.on('data', (chunk) => {
            let isDraining = sendMessageToSocket(chunk, this.socket)
            if (!isDraining) {
                this.pauseStreams()
            }
        })

        stream.stream.on('end', () => {
            log(`finished source stream ${stream.name}`)
            this.streams = this.streams.filter(s => s != stream)

            if (this.streams.length == 0)
                log(`FINISHED WORK !`)
        })
    }

    start() {
        let askShaStatusPayloadsStream = new AskShaStatusPayloadsStream(this)
        this.addStream("FILES AND DIRECTORY LISTING", askShaStatusPayloadsStream)
        this.addStream("ADD SHA IN TRANSACTION", this.addShaInTxPayloadsStream)

        askShaStatusPayloadsStream.on('end', () => {
            this.resumeStreams()
            this.addShaInTxPayloadsStream.end()
        })

        let directoryLister = new DirectoryLister('d:\\tmp\\tmp', ['.git', 'exp-cache'])
        let shaProcessor = new ShaProcessor()
        directoryLister
            .pipe(shaProcessor)
            .pipe(askShaStatusPayloadsStream)

        this.socket.on('drain', () => {
            this.resumeStreams()
        })

        this.socket.on('message', (message) => {
            let [messageType, content] = Serialization.deserialize(message, null)

            switch (messageType) {
                case MSG_TYPE_REP_SHA_STATUS:
                    let sha = content[0]
                    let size = content[1]

                    if (this.pendingAskShaStatus.length == 0) {
                        log.err(`error, received an unknown SHA size !`)
                        return
                    }

                    let matchedPending = this.pendingAskShaStatus.shift()
                    if (matchedPending.contentSha != sha) {
                        log.err(`error, received a non matched SHA size ! ${matchedPending.contentSha} / ${sha}`)
                        return
                    }

                    log(`received ${sha} remote size, still waiting for ${this.pendingAskShaStatus.length}`)
                    if (matchedPending.size != size) {
                        /*if (size < matchedPending.size)
                            pendingSendShaBytes.push({ fileInfo: matchedPending, offset: size })
                        else {
                            log(`warning : remote sha ${sha} is bigger than expected, restarting transfer`)
                            pendingSendShaBytes.push({ fileInfo: matchedPending, offset: 0 })
                        }*/
                        log(`SHOULD UPLOAD SOME BYTES !`)
                    }
                    else {
                        this.addShaInTxPayloadsStream.write(matchedPending)
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
    }

    addToTransaction(fileAndShaInfo: FileAndShaInfo) {
        this.addShaInTxPayloadsStream.write(fileAndShaInfo)
    }

    addPendingAskShaStatus(fileAndShaInfo: FileAndShaInfo) {
        this.pendingAskShaStatus.push(fileAndShaInfo)
    }
}

class AskShaStatusPayloadsStream extends Stream.Transform {
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
            this.push(Serialization.serialize([MSG_TYPE_ASK_SHA_STATUS, fileAndShaInfo.contentSha]))
            this.clientStatus.addPendingAskShaStatus(fileAndShaInfo)
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



