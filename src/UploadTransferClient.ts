import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as FS from 'fs'
import * as Stream from 'stream'
import * as Net from 'net'
import * as Serialization from './serialisation'
import { ShaCache } from './ShaCache'
import { HexaBackupStore } from './HexaBackupStore'
import * as Model from './Model'
import * as UploadTransferModel from './UploadTransferModel'
import * as Socket2Message from './Socket2Message'

const log = require('./Logger')('UploadTransferClient')

export type ReadableStream = Stream.Readable | Stream.Transform

export interface StreamInfo {
    name: string;
    stream: ReadableStream;
    blocking: boolean;
}

class ShaProcessor extends Stream.Transform {
    constructor(private shaCache: ShaCache) {
        super({ objectMode: true })
    }

    _flush(callback) {
        callback()
    }

    async _transform(chunk: UploadTransferModel.FileInfo, encoding, callback: (err, data) => void) {
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

        let pushedSome = false

        while (!pushedSome && this.stack.length > 0) {
            let currentPath = this.stack.pop();

            this.awaitingReaddir = true
            let files = await FsTools.readDir(currentPath);
            this.awaitingReaddir = false

            let filesDesc = files
                .filter((fileName) => !this.ignoredNames.some(name => fileName == name))
                .map(fileName => {
                    let fullFileName = fsPath.join(currentPath, fileName);
                    let stat = fs.statSync(fullFileName);

                    return {
                        name: fullFileName,
                        isDirectory: stat.isDirectory(),
                        lastWrite: stat.mtime.getTime(),
                        size: stat.isDirectory() ? 0 : stat.size
                    }
                })

            if (filesDesc.length == 0)
                continue

            // important to push directories first because push is reentrant and we might find ourselves thinking the work is finished
            filesDesc.filter(desc => desc.isDirectory)
                .forEach(desc => {
                    this.stack.push(desc.name)
                })

            for (let desc of filesDesc) {
                this.push(desc)
                pushedSome = true
            }
        }

        if (!this.awaitingReaddir && this.stack.length == 0) {
            this.push(null)
        }
    }
}

class AddShaInTxPayloadsStream extends Stream.Transform {
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

/**
 * Receives a file's raw bytes and send them by block
 */
class ShaBytesPayloadsStream extends Stream.Transform {
    constructor(private clientStatus: UploadTransferClient, private fileInfo: UploadTransferModel.FileAndShaInfo, private offset: number) {
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
        this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES_COMMIT, this.fileInfo.contentSha]))
        callback(null, null)
    }

    async _transform(buffer: Buffer, encoding, callback: (err, data) => void) {
        this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES, this.fileInfo.contentSha, this.offset, buffer]))
        this.offset += buffer.length

        callback(null, null)
    }
}

class AskShaStatusPayloadsStream extends Stream.Transform {
    private nextAskShaStatusReqId = 1

    constructor(private clientStatus: UploadTransferClient) {
        super({ objectMode: true })
    }

    _flush(callback) {
        callback()
    }

    async _transform(fileAndShaInfo: UploadTransferModel.FileAndShaInfo, encoding, callback: (err, data) => void) {
        if (fileAndShaInfo.isDirectory)
            this.clientStatus.addToTransaction(fileAndShaInfo)
        else {
            let reqId = this.nextAskShaStatusReqId++
            this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_ASK_SHA_STATUS, fileAndShaInfo.contentSha, reqId]))
            this.clientStatus.addPendingAskShaStatus(reqId, fileAndShaInfo)
        }

        callback(null, null)
    }
}







export class UploadTransferClient {
    streams: StreamInfo[] = []

    addShaInTxPayloadsStream

    pendingAskShaStatus: Map<number, UploadTransferModel.FileAndShaInfo> = new Map()
    pendingSendShaBytes: { fileInfo: UploadTransferModel.FileAndShaInfo; offset: number; }[] = []

    ignoredDirs = ['.hb-cache', '.hb-object', '.hb-refs', '.metadata', '.settings', '.idea', 'target', 'node_modules', 'gwt-unitCache', '.ntvs_analysis.dat', '.gradle', 'student_pictures', 'logs']

    private isNetworkDraining: boolean = true

    constructor(private pushedDirectory: string, private sourceId: string, private socket: Net.Socket) {
        this.addShaInTxPayloadsStream = new AddShaInTxPayloadsStream(pushedDirectory)
    }

    private addStream(name: string, blocking: boolean, stream: ReadableStream) {
        let si = {
            name,
            stream,
            blocking
        }

        this.streams.push(si)

        log(`added stream ${si.name}, count=${this.streams.length}`)

        this.initStream(si)
    }

    private initStream(stream: StreamInfo) {
        stream.stream.on('end', () => {
            this.isNetworkDraining = true

            this.streams = this.streams.filter(s => s != stream)
            log(`finished source stream ${stream.name}, ${this.streams.length} left`)

            if (this.streams.length == 0)
                log(`FINISHED WORK !`)
            else
                this.maybeSendBytesToNetwork()

            this.maybeCloseAddInTxStream()
        })

        stream.stream.on('readable', () => {
            this.maybeSendBytesToNetwork()
        })
    }

    private maybeSendBytesToNetwork() {
        if (!this.isNetworkDraining)
            return

        for (let i = this.streams.length - 1; i >= 0; i--) {
            let si = this.streams[i]

            while (this.isNetworkDraining) {
                let chunk = si.stream.read(1)
                if (chunk == null) {
                    break
                }

                this.isNetworkDraining = Socket2Message.sendMessageToSocket(chunk, this.socket)
            }

            if (si.blocking)
                break
        }
    }

    private moreAskShaStatusToCome = true

    private isAddInTxStreamToBeClosed() {
        return this.addShaInTxPayloadsStream
            && (!this.moreAskShaStatusToCome)
            && (!this.streams.some(si => si.stream instanceof ShaBytesPayloadsStream))
            && (this.pendingAskShaStatus.size == 0)
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
        let directoryLister = new DirectoryLister(this.pushedDirectory, this.ignoredDirs)

        log(`preparing...`)

        let total = {
            nbDirectories: 0,
            nbFiles: 0,
            bytes: 0
        }

        directoryLister.on('end', () => {
            log(`prepared to send ${total.nbDirectories} directories, ${total.nbFiles} files and ${total.bytes / (1024 * 1024 * 1024)} Gb`)
            this.startSending()
        })

        directoryLister.on('data', (file: UploadTransferModel.FileInfo) => {
            total.nbDirectories += file.isDirectory ? 1 : 0
            total.nbFiles += file.isDirectory ? 0 : 1
            total.bytes += file.size
        })
    }

    private startSending() {
        this.socket.on('drain', () => {
            this.isNetworkDraining = true
            this.maybeSendBytesToNetwork()
        })

        let registeredShasForSending = new Set<string>()

        this.socket.on('message', (message) => {
            let [messageType, content] = Serialization.deserialize(message, null)

            switch (messageType) {
                case UploadTransferModel.MSG_TYPE_REP_BEGIN_TX: {
                    let txId = content

                    log(`good news, we are beginning transaction ${txId}`)

                    let askShaStatusPayloadsStream = new AskShaStatusPayloadsStream(this)
                    this.addStream("AskShaStatus", false, askShaStatusPayloadsStream)
                    this.addStream("AddShaInTransaction", false, this.addShaInTxPayloadsStream)

                    askShaStatusPayloadsStream.on('end', () => {
                        this.moreAskShaStatusToCome = false
                        this.maybeCloseAddInTxStream()
                    })

                    let directoryLister = new DirectoryLister(this.pushedDirectory, this.ignoredDirs)
                    let shaProcessor = new ShaProcessor(new ShaCache(fsPath.join(this.pushedDirectory, '.hb-cache')))
                    directoryLister
                        .pipe(shaProcessor)
                        .pipe(askShaStatusPayloadsStream)

                    this.isNetworkDraining = true
                    this.maybeSendBytesToNetwork()
                    break
                }

                case UploadTransferModel.MSG_TYPE_REP_SHA_STATUS:
                    let reqId = content[0]
                    let size = content[1]

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

                        this.addStream(`FileTransfert from ${offset} ${matchedPending.name} `, true, new ShaBytesPayloadsStream(this, matchedPending, offset))
                    }
                    else {
                        this.addToTransaction(matchedPending)
                    }

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

        let clientId = "test"

        this.isNetworkDraining = Socket2Message.sendMessageToSocket(Serialization.serialize([UploadTransferModel.MSG_TYPE_ASK_BEGIN_TX, clientId]), this.socket)
    }

    addToTransaction(fileAndShaInfo: UploadTransferModel.FileAndShaInfo) {
        this.addShaInTxPayloadsStream.write(fileAndShaInfo)
    }

    addPendingAskShaStatus(reqId: number, fileAndShaInfo: UploadTransferModel.FileAndShaInfo) {
        this.pendingAskShaStatus.set(reqId, fileAndShaInfo)
    }
}