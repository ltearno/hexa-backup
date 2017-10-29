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

export class AskShaStatusStream extends Stream.Transform {
    private waitedShas = new Map<string, UploadTransferModel.FileAndShaInfo>()
    private sentShas = new Set<string>()

    private sourceStreamEnded = false

    constructor(private backupedDirectory: string, private streamExecutor: StreamStack) {
        super({ objectMode: true })
    }

    initSourceStream(sourceStream) {
        sourceStream.pipe(this, { end: false })
        sourceStream.on('end', () => {
            this.sourceStreamEnded = true
            this.maybeClose()
        })
    }

    _flush(callback) {
        callback()
    }

    receivedShaInformation(sha: string, size: number) {
        if (!this.waitedShas.has(sha)) {
            log.err(`error, received a non matched SHA size for ${sha} : ${size}`)
            return
        }

        let info = this.waitedShas.get(sha)
        this.waitedShas.delete(sha)

        if (info.size == size) {
            this.push(createAddShaInTxMessage(info, this.backupedDirectory))
        }
        else {
            let offset = size
            if (size > info.size) {
                log(`warning : remote sha ${info.contentSha} is bigger than expected, restarting transfer`)
                offset = 0
            }

            this.streamExecutor.addStream(`${info.contentSha.substring(0, 5)} - ${info.name}`, new ShaBytesStream(info, offset, this.backupedDirectory))
        }

        this.maybeClose()
    }

    _transform(fileAndShaInfo: UploadTransferModel.FileAndShaInfo, encoding, callback: () => void) {
        if (this.sentShas.has(fileAndShaInfo.contentSha)) {
            this.push(createAddShaInTxMessage(fileAndShaInfo, this.backupedDirectory))
        }
        else {
            this.sentShas.add(fileAndShaInfo.contentSha)

            if (fileAndShaInfo.isDirectory) {
                this.push(createAddShaInTxMessage(fileAndShaInfo, this.backupedDirectory))
            }
            else {
                this.waitedShas.set(fileAndShaInfo.contentSha, fileAndShaInfo)
                this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_ASK_SHA_STATUS, fileAndShaInfo.contentSha]))
            }
        }

        callback()
    }

    private maybeClose() {
        if (!this.waitedShas.size && this.sourceStreamEnded)
            this.push(null)
    }
}

function createAddShaInTxMessage(fileAndShaInfo: UploadTransferModel.FileAndShaInfo, backupedDirectory: string) {
    let descriptor: Model.FileDescriptor = {
        contentSha: fileAndShaInfo.contentSha,
        isDirectory: fileAndShaInfo.isDirectory,
        lastWrite: fileAndShaInfo.lastWrite,
        size: fileAndShaInfo.size,
        name: fsPath.relative(backupedDirectory, fileAndShaInfo.name)
    }

    return Serialization.serialize([UploadTransferModel.MSG_TYPE_ADD_SHA_IN_TX, descriptor])
}

export class ShaBytesStream extends Stream.Transform {
    private fileStream: Stream.Readable

    constructor(private fileInfo: UploadTransferModel.FileAndShaInfo, private offset: number, private backupedDirectory: string) {
        super()

        let fsAny = fs as any
        this.fileStream = fsAny.createReadStream(this.fileInfo.name, {
            flags: 'r',
            encoding: null,
            start: this.offset
        })

        this.fileStream.pipe(this)
    }

    _flush(callback) {
        this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES_COMMIT, this.fileInfo.contentSha]))
        this.push(createAddShaInTxMessage(this.fileInfo, this.backupedDirectory))
        callback()
    }

    _transform(data, encoding, callback) {
        if (data) {
            this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES, this.fileInfo.contentSha, this.offset, data]))
            this.offset += data.length
        }
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
        if (this.streams.length)
            this.streams[this.streams.length - 1].stream.pause()

        this.streams.push({ name, stream })

        log(`[${this.streams.length}] added stream ${name}`)

        stream.pipe(this, { end: false })
        stream.on('end', () => {
            log(`finishedStream ${name}`)
            this.streams = this.streams.filter(si => si.stream != stream)

            if (this.streams.length)
                this.streams[this.streams.length - 1].stream.resume()

            this.maybeClose()
        })
    }

    closeWhenEmpty() {
        this._closeWhenEmpty = true
        this.maybeClose()
    }

    _transform(data, encoding, callback) {
        callback(null, data)
    }

    private maybeClose() {
        if (this._closeWhenEmpty && !this.streams.length)
            this.push(null)
    }
}



const GIGABYTE = 1024 * 1024 * 1024

export class UploadTransferClient {
    private streamStack: StreamStack
    private askShaStatusPayloadsStream: AskShaStatusStream

    private ignoredDirs = ['.hb-cache', '.hb-object', '.hb-refs', '.metadata', '.settings', '.idea', 'target', 'node_modules', 'gwt-unitCache', '.ntvs_analysis.dat', '.gradle', 'student_pictures', 'logs']

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
            //message: `${this.status.phase}, ${this.status.nbAddedInTx}/${this.status.toSync.nbDirectories + this.status.toSync.nbFiles} added files, ${this.status.nbBytesInTx / GIGABYTE}/${this.status.toSync.nbBytes / GIGABYTE} Gb, ${this.pendingAskShaStatus.size} pending sha status, ${this.status.shaBytesSent / GIGABYTE} sha Gb sent`,
            message: `${this.status.phase}, ${this.status.nbAddedInTx}/${this.status.toSync.nbDirectories + this.status.toSync.nbFiles} added files, ${this.status.nbBytesInTx / GIGABYTE}/${this.status.toSync.nbBytes / GIGABYTE} Gb, ${this.status.shaBytesSent / GIGABYTE} sha Gb sent`,
            completed: this.status.toSync.nbBytes > 0 ? (this.status.nbBytesInTx / this.status.toSync.nbBytes) : 0
        }
    }

    constructor(private pushedDirectory: string, private sourceId: string, private socket: Net.Socket) { }

    start() {
        log.setStatus(() => this.giveStatus())

        this.status.phase = 'preparing'

        if (1 * 1 == 1) {
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

        this.streamStack = new StreamStack()
        this.streamStack
            .pipe(new Socket2Message.MessageToPayloadStream())
            .pipe(this.socket, { end: false })

        this.streamStack.on('end', () => {
            Socket2Message.sendMessageToSocket(Serialization.serialize([UploadTransferModel.MSG_TYPE_COMMIT_TX]), this.socket)
            log(`upload finished`)
        })

        this.socket.on('message', (message) => {
            let [messageType, content] = Serialization.deserialize(message, null)

            switch (messageType) {
                case UploadTransferModel.MSG_TYPE_REP_BEGIN_TX: {
                    let txId = content

                    log(`starting transaction ${txId}`)

                    let shaCache = new ShaCache.ShaCache(fsPath.join(this.pushedDirectory, '.hb-cache'))

                    let shaProcessor = new ShaProcessor.ShaProcessor(shaCache)
                    let directoryLister = new DirectoryLister.DirectoryLister(this.pushedDirectory, this.ignoredDirs)

                    this.askShaStatusPayloadsStream = new AskShaStatusStream(this.pushedDirectory, this.streamStack)
                    this.askShaStatusPayloadsStream.initSourceStream(directoryLister.pipe(shaProcessor))

                    this.streamStack.addStream("AskShaStatus", this.askShaStatusPayloadsStream)
                    this.streamStack.closeWhenEmpty()
                    this.streamStack.on('end', () => shaCache.persist())
                    break
                }

                case UploadTransferModel.MSG_TYPE_REP_SHA_STATUS:
                    let [sha, size] = content

                    this.askShaStatusPayloadsStream.receivedShaInformation(sha, size)
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
    }
}