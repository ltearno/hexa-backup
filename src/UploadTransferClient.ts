import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as FS from 'fs'
import * as Stream from 'stream'
import * as Net from 'net'
import * as Serialization from './serialisation'
import * as ShaCache from './ShaCache'
import * as HexaBackupStore from './HexaBackupStore'
import * as Model from './Model'
import * as UploadTransferModel from './UploadTransferModel'
import * as Socket2Message from './Socket2Message'
import * as DirectoryLister from './directory-lister'
import * as ShaProcessor from './sha-processor'

const log = require('./Logger')('UploadTransferClient')

export type ReadableStream = Stream.Readable | Stream.Transform

class AskShaStatusStream extends Stream.Transform {
    private nextAskShaStatusReqId = 1

    constructor(private clientStatus: UploadTransferClient) {
        super({ objectMode: true })
    }

    _flush(callback) {
        callback()
    }

    async _transform(fileAndShaInfo: UploadTransferModel.FileAndShaInfo, encoding, callback: (err, data) => void) {
        if (fileAndShaInfo.isDirectory) {
            this.clientStatus.addToTransaction(fileAndShaInfo)
        }
        else {
            let reqId = this.nextAskShaStatusReqId++
            this.clientStatus.addPendingAskShaStatus(reqId, fileAndShaInfo)

            this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_ASK_SHA_STATUS, fileAndShaInfo.contentSha, reqId]))
        }

        callback(null, null)
    }
}

class AddShaInTxStream extends Stream.Transform {
    constructor(private backupedDirectory: string) {
        super({ objectMode: true })
    }

    _flush(callback) {
        callback()
    }

    async _transform(fileAndShaInfo: UploadTransferModel.FileAndShaInfo, encoding, callback: (err, data) => void) {
        let descriptor: Model.FileDescriptor

        descriptor = {
            contentSha: fileAndShaInfo.contentSha,
            isDirectory: fileAndShaInfo.isDirectory,
            lastWrite: fileAndShaInfo.lastWrite,
            size: fileAndShaInfo.size,
            name: fsPath.relative(this.backupedDirectory, fileAndShaInfo.name)
        }

        callback(null, Serialization.serialize([UploadTransferModel.MSG_TYPE_ADD_SHA_IN_TX, descriptor]))
    }
}

class ShaBytesPayloadsStream extends Stream.Readable {
    private fileBytesStream = null
    private currentItem = null
    private waitingReadable = false
    private fileQueue: { fileInfo: UploadTransferModel.FileAndShaInfo; offset: number; }[] = []

    constructor(private client: UploadTransferClient) {
        super({ objectMode: true })
    }

    enqueueFile(fileInfo: UploadTransferModel.FileAndShaInfo, offset: number) {
        this.fileQueue.push({ fileInfo, offset })
        if (this.waitingReadable)
            this.readAndMaybePushBuffer()
    }

    endQueue() {
        this.push(null)
    }

    _read(size) {
        if (this.fileBytesStream == null || this.currentItem == null) {
            if (this.fileQueue.length == 0) {
                this.waitingReadable = true
                return
            }

            this.currentItem = this.fileQueue.shift()

            let fsAny = fs as any
            this.fileBytesStream = fsAny.createReadStream(this.currentItem.fileInfo.name, {
                flags: 'r',
                encoding: null,
                start: this.currentItem.offset
            })

            this.fileBytesStream.on('end', () => {
                this.client.addToTransaction(this.currentItem.fileInfo)
                this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES_COMMIT, this.currentItem.fileInfo.contentSha]))

                this.currentItem = null
                this.fileBytesStream = null
            })

            this.fileBytesStream.on('readable', () => {
                if (this.waitingReadable)
                    this.readAndMaybePushBuffer()
            })
        }

        this.readAndMaybePushBuffer()
    }

    private readAndMaybePushBuffer() {
        if (this.fileBytesStream) {
            this.waitingReadable = false
            let buffer: Buffer = this.fileBytesStream.read()
            if (buffer != null)
                this.pushBuffer(buffer)
            else
                this.waitingReadable = true
        }
        else {
            this.waitingReadable = true
        }
    }

    private pushBuffer(buffer: Buffer) {
        this.client.status.shaBytesSent += buffer.byteLength

        this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES, this.currentItem.fileInfo.contentSha, this.currentItem.offset, buffer]))
        this.currentItem.offset += buffer.length
    }
}

export class ShaBytesStream extends Stream.Transform {
    private fileStream: Stream.Readable

    constructor(private fileInfo: UploadTransferModel.FileAndShaInfo, private offset: number) {
        super()

        let fsAny = fs as any
        this.fileStream = fsAny.createReadStream(this.fileInfo.name, {
            flags: 'r',
            encoding: null,
            start: this.offset
        })

        this.fileStream.pipe(this, { end: false })
        this.fileStream.on('end', () => {
            this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES_COMMIT, this.fileInfo.contentSha]))
            this.push(null)
        })
    }

    _transform(data, encoding, callback) {
        if (data)
            this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES, this.fileInfo.contentSha, this.offset, data]))
        callback(null, null)
    }
}


export interface StreamInfo {
    name: string
    stream: ReadableStream
    streamEndCallback: () => void
}

export class StreamStack extends Stream.Transform {
    private streams: {
        name: string
        stream: ReadableStream
    }[] = []

    private _closeWhenEmpty = false

    constructor() {
        super({ objectMode: true })
    }

    addStream(name: string, stream: ReadableStream) {
        if (this.streams.length) {
            log(`pause ${this.streams[this.streams.length - 1].name}`)
            this.streams[this.streams.length - 1].stream.pause()
        }

        this.streams.push({ name, stream })

        log(`[${this.streams.length}] added stream ${name}`)

        stream.pipe(this, { end: false })
        stream.on('end', () => {
            log(`finishedStream ${name}`)
            this.streams = this.streams.filter(si => si.stream != stream)

            if (this.streams.length) {
                log(`resume ${this.streams[this.streams.length - 1].name}`)
                this.streams[this.streams.length - 1].stream.resume()
            }

            if (this._closeWhenEmpty && !this.streams.length) {
                log(`work finished`)
                this.push(null)
            }
        })
    }

    closeWhenEmpty() {
        this._closeWhenEmpty = true
        if (!this.streams.length)
            this.push(null)
    }

    _transform(data, encoding, callback) {
        callback(null, data)
    }
}

/**
 * Stores a pool of readable streams.
 * 
 * The stream ends when there is no more stream and no response to wait for
 */
export class StreamEngine {
    private streams: StreamInfo[] = []
    private isNetworkDraining: boolean = true

    // returns if we should consider draining after that
    private dataReadyCallback: (chunk: any) => boolean = null

    onDataReady(callback: (chunk: any) => boolean) {
        this.dataReadyCallback = callback
    }

    addStream(name: string, stream: ReadableStream, streamEndCallback: () => void) {
        let si = {
            name,
            stream,
            streamEndCallback
        }

        this.streams.push(si)
        log.dbg(`added stream ${si.name}, count=${this.streams.length}`)
        this.initStream(si)
    }

    /**
     * Asks for data
     */
    drain() {
        this.isNetworkDraining = true
    }

    maybeSendBytesToNetwork() {
        if (!this.isNetworkDraining)
            return

        for (let i = this.streams.length - 1; i >= 0; i--) {
            let si = this.streams[i]

            while (this.isNetworkDraining) {
                //if (i != this.streams.length - 1)
                //    log(`try read from s depth ${this.streams.length - 1 - i}`)
                let chunk = si.stream.read()
                if (!chunk)
                    break

                this.isNetworkDraining = this.dataReadyCallback(chunk)
            }
        }
    }

    private initStream(stream: StreamInfo) {
        stream.stream.on('end', () => {
            this.isNetworkDraining = true

            this.streams = this.streams.filter(s => s != stream)
            log.dbg(`finished source stream ${stream.name}, ${this.streams.length} left`)

            if (this.streams.length == 0)
                log(`FINISHED WORK !`)
            else
                this.maybeSendBytesToNetwork()

            stream.streamEndCallback()
        })

        stream.stream.on('readable', () => {
            this.maybeSendBytesToNetwork()
        })
    }
}



const GIGABYTE = 1024 * 1024 * 1024

export class UploadTransferClient {
    private streamEngine = new StreamEngine()

    private addShaInTxPayloadsStream
    private shaBytesPayloadsStream: ShaBytesPayloadsStream = null
    private pendingAskShaStatus: Map<number, UploadTransferModel.FileAndShaInfo> = new Map()
    private pendingSendShaBytes: { fileInfo: UploadTransferModel.FileAndShaInfo; offset: number; }[] = []

    private ignoredDirs = []// ['.hb-cache', '.hb-object', '.hb-refs', '.metadata', '.settings', '.idea', 'target', 'node_modules', 'gwt-unitCache', '.ntvs_analysis.dat', '.gradle', 'student_pictures', 'logs']

    status = {
        phase: "uninitialized",
        toSync: {
            nbFiles: 0,
            nbDirectories: 0,
            nbBytes: 0
        },
        shaBytesSent: 0,
        nbAddedInTx: 0, // nb files & dirs in tx
        nbBytesInTx: 0 // equivalent of number of bytes of content actually in the tx
    }

    private giveStatus() {
        return {
            message: `${this.status.phase}, ${this.status.nbAddedInTx}/${this.status.toSync.nbDirectories + this.status.toSync.nbFiles} added files, ${this.status.nbBytesInTx / GIGABYTE}/${this.status.toSync.nbBytes / GIGABYTE} Gb, ${this.pendingAskShaStatus.size} pending sha status, ${this.status.shaBytesSent / GIGABYTE} sha Gb sent`,
            completed: this.status.toSync.nbBytes > 0 ? (this.status.nbBytesInTx / this.status.toSync.nbBytes) : 0
        }
    }

    constructor(private pushedDirectory: string, private sourceId: string, private socket: Net.Socket) {
        this.addShaInTxPayloadsStream = new AddShaInTxStream(pushedDirectory)
        this.shaBytesPayloadsStream = new ShaBytesPayloadsStream(this)
    }

    private moreAskShaStatusToCome = true

    private maybeCloseShaBytesStream() {
        if (!this.moreAskShaStatusToCome && this.pendingAskShaStatus.size == 0) {
            this.shaBytesPayloadsStream.endQueue()
        }
    }

    private isAddInTxStreamToBeClosed() {
        return this.addShaInTxPayloadsStream
            && (!this.moreAskShaStatusToCome)
            && (this.pendingAskShaStatus.size == 0)
            && (!this.shaBytesPayloadsStream)
    }

    private maybeCloseAddInTxStream() {
        if (this.isAddInTxStreamToBeClosed()) {
            this.addShaInTxPayloadsStream.end()
            this.addShaInTxPayloadsStream = null

            Socket2Message.sendMessageToSocket(Serialization.serialize([UploadTransferModel.MSG_TYPE_COMMIT_TX]), this.socket)
            //this.socket.end() // server will do that ;)
        }
    }

    start() {
        log.setStatus(() => this.giveStatus())

        this.status.phase = 'preparing'

        if (0 * 1 == 1) {
            this.startSending()
        }
        else {
            let directoryLister = new DirectoryLister.DirectoryLister(this.pushedDirectory, this.ignoredDirs)

            directoryLister.on('end', () => {
                log(`prepared to send ${this.status.toSync.nbDirectories} directories, ${this.status.toSync.nbFiles} files and ${this.status.toSync.nbBytes / (1024 * 1024 * 1024)} Gb`)
                this.startSending()
            })

            directoryLister.on('data', (file: UploadTransferModel.FileInfo) => {
                this.status.toSync.nbDirectories += file.isDirectory ? 1 : 0
                this.status.toSync.nbFiles += file.isDirectory ? 0 : 1
                this.status.toSync.nbBytes += file.size
            })
        }
    }

    private startSending() {
        this.status.phase = 'sending'

        this.streamEngine.onDataReady((chunk) => Socket2Message.sendMessageToSocket(chunk, this.socket))

        this.socket.on('drain', () => {
            this.streamEngine.drain()
            this.streamEngine.maybeSendBytesToNetwork()
        })

        let registeredShasForSending = new Set<string>()

        this.socket.on('message', (message) => {
            let [messageType, content] = Serialization.deserialize(message, null)

            switch (messageType) {
                case UploadTransferModel.MSG_TYPE_REP_BEGIN_TX: {
                    let txId = content

                    log(`starting transaction ${txId}`)

                    let askShaStatusPayloadsStream = new AskShaStatusStream(this)
                    this.streamEngine.addStream("AskShaStatus", askShaStatusPayloadsStream, () => this.maybeCloseAddInTxStream())
                    this.streamEngine.addStream("AddShaInTransaction", this.addShaInTxPayloadsStream, () => this.maybeCloseAddInTxStream())
                    this.streamEngine.addStream("ShaBytes", this.shaBytesPayloadsStream, () => this.maybeCloseAddInTxStream())

                    askShaStatusPayloadsStream.on('end', () => {
                        this.moreAskShaStatusToCome = false
                        this.maybeCloseShaBytesStream()
                        this.maybeCloseAddInTxStream()
                    })

                    this.shaBytesPayloadsStream.on('end', () => {
                        this.shaBytesPayloadsStream = null
                        this.maybeCloseAddInTxStream()
                    })

                    let directoryLister = new DirectoryLister.DirectoryLister(this.pushedDirectory, this.ignoredDirs)
                    let shaProcessor = new ShaProcessor.ShaProcessor(new ShaCache.ShaCache(fsPath.join(this.pushedDirectory, '.hb-cache')))
                    directoryLister
                        .pipe(shaProcessor)
                        .pipe(askShaStatusPayloadsStream)

                    this.streamEngine.drain()
                    this.streamEngine.maybeSendBytesToNetwork()
                    break
                }

                case UploadTransferModel.MSG_TYPE_REP_SHA_STATUS:
                    let [reqId, size] = content

                    let matchedPending = this.pendingAskShaStatus.get(reqId)
                    if (!matchedPending) {
                        log.err(`error, received a non matched SHA size, reqId = ${reqId}`)
                        return
                    }

                    this.pendingAskShaStatus.delete(reqId)

                    //log(`received ${sha } remote size, still waiting for ${this.pendingAskShaStatus.length }`)
                    if (matchedPending.size != size && !registeredShasForSending.has(matchedPending.contentSha)) {
                        registeredShasForSending.add(matchedPending.contentSha)

                        let offset = size
                        if (size > matchedPending.size) {
                            log(`warning : remote sha ${matchedPending.contentSha} is bigger than expected, restarting transfer`)
                            offset = 0
                        }

                        this.shaBytesPayloadsStream.enqueueFile(matchedPending, offset)
                    }
                    else {
                        this.addToTransaction(matchedPending)
                    }

                    this.maybeCloseShaBytesStream()
                    this.maybeCloseAddInTxStream()

                    break

                default:
                    log.err(`received unknown msg type ${messageType}`)
            }
        })

        this.socket.on('close', () => {
            log('connection to server closed')
        })

        Socket2Message.socketDataToMessage(this.socket)

        Socket2Message.sendMessageToSocket(Serialization.serialize([UploadTransferModel.MSG_TYPE_ASK_BEGIN_TX, this.sourceId]), this.socket)
        this.streamEngine.drain()
    }

    addToTransaction(fileAndShaInfo: UploadTransferModel.FileAndShaInfo) {
        this.status.nbAddedInTx++
        this.status.nbBytesInTx += fileAndShaInfo.size

        this.addShaInTxPayloadsStream.write(fileAndShaInfo)
    }

    addPendingAskShaStatus(reqId: number, fileAndShaInfo: UploadTransferModel.FileAndShaInfo) {
        this.pendingAskShaStatus.set(reqId, fileAndShaInfo)
    }
}