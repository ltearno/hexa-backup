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
import Log from './log'

const log = Log('UploadTransferClient')

export type ReadableStream = Stream.Readable | Stream.Transform

export class AskShaStatusStream extends Stream.Transform {
    private TRIGGER_HIGH_WAITEDSHAS = 100
    private TRIGGER_LOW_WAITEDSHAS = 30
    private sourceInPause = false

    waitedShas = new Map<string, UploadTransferModel.FileAndShaInfo>()
    private sentShas = new Set<string>()

    sourceStream: ReadableStream = null

    private toSendFiles: { fileInfo: UploadTransferModel.FileAndShaInfo, offset: number }[] = []

    fileStream: ShaBytesStream = null

    private finished = false

    constructor(private backupedDirectory: string, private status: UploadStatus) {
        super({ objectMode: true, highWaterMark: 100 })
    }

    initSourceStream(sourceStream: ReadableStream) {
        let transform = new Stream.Transform({ objectMode: true })
        transform._transform = (fileAndShaInfo: UploadTransferModel.FileAndShaInfo, encoding, callback: () => void) => {
            if (this.sentShas.has(fileAndShaInfo.contentSha)) {
                this.status.nbAddedInTx++
                this.status.nbBytesInTx += fileAndShaInfo.size
                this.push(createAddShaInTxMessage(fileAndShaInfo, this.backupedDirectory))
            }
            else {
                this.sentShas.add(fileAndShaInfo.contentSha)

                if (fileAndShaInfo.isDirectory) {
                    this.status.nbAddedInTx++
                    this.push(createAddShaInTxMessage(fileAndShaInfo, this.backupedDirectory))
                }
                else {
                    this.waitedShas.set(fileAndShaInfo.contentSha, fileAndShaInfo)
                    this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_ASK_SHA_STATUS, fileAndShaInfo.contentSha]))

                    if (this.waitedShas.size > this.TRIGGER_HIGH_WAITEDSHAS && !this.sourceInPause) {
                        this.sourceInPause = true
                        this.sourceStream.pause()
                    }
                }
            }

            callback()
        }

        this.sourceStream = sourceStream
        this.sourceStream.pipe(transform).pipe(this, { end: false })
        this.sourceStream.on('end', () => {
            log(`finished listing files and hashing`)
            transform.end()
            this.sourceStream = null
            this.updateQueue()
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
            this.status.nbAddedInTx++
            this.status.nbBytesInTx += info.size
            this.push(createAddShaInTxMessage(info, this.backupedDirectory))
        }
        else {
            let offset = size
            if (size > info.size) {
                log(`warning : remote sha ${info.contentSha} is bigger than expected, restarting transfer`)
                offset = 0
            }

            this.toSendFiles.push({ fileInfo: info, offset })
        }

        this.updateQueue()
    }

    private updateQueue() {
        if (this.fileStream) {
            return
        }
        else if (this.toSendFiles.length) {
            if (this.sourceStream) {
                this.sourceStream.pause()
                this.sourceInPause = true
            }

            let fileInfo = this.toSendFiles.shift()
            this.fileStream = new ShaBytesStream(fileInfo.fileInfo, fileInfo.offset, this.backupedDirectory, this.status)
            this.fileStream.pipe(this, { end: false })
            this.fileStream.on('end', () => {
                this.fileStream = null
                this.updateQueue()
            })

            this.status.phase = `sending sha ${fileInfo.fileInfo.contentSha.substring(0, 5)} ${fileInfo.fileInfo.name.substring(-20)} @ ${fileInfo.offset} (sz:${fileInfo.fileInfo.size}), ${this.toSendFiles.length} files in queue`
            return
        }
        else if (this.sourceStream) {
            if (this.sourceInPause) {
                if (this.waitedShas.size <= this.TRIGGER_LOW_WAITEDSHAS) {
                    this.sourceStream.resume()
                    this.sourceInPause = false
                    this.status.phase = `parsing directories, hashing files and asking remote status`
                }
                else {
                    this.status.phase = `waiting for shas status`
                }
            }
        }
        else if (!this.waitedShas.size && !this.finished) {
            this.finished = true
            this.status.phase = 'finished transfer'
            this.push(null)
        }
    }

    _transform(data, encoding, callback: () => void) {
        this.push(data)
        callback()
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

    constructor(private fileInfo: UploadTransferModel.FileAndShaInfo, private offset: number, private backupedDirectory: string, private status: UploadStatus) {
        super({ objectMode: true, highWaterMark: 100 })

        let fsAny = fs as any
        this.fileStream = fsAny.createReadStream(this.fileInfo.name, {
            flags: 'r',
            encoding: null,
            start: this.offset,
            autoClose: true
        })

        this.fileStream.pipe(this)
    }

    _flush(callback) {
        this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES_COMMIT, this.fileInfo.contentSha]))
        this.status.nbShaSent++
        this.status.nbAddedInTx++
        this.status.nbBytesInTx += this.fileInfo.size
        this.push(createAddShaInTxMessage(this.fileInfo, this.backupedDirectory))
        callback()
    }

    _transform(data, encoding, callback) {
        if (data) {
            this.push(Serialization.serialize([UploadTransferModel.MSG_TYPE_SHA_BYTES, this.fileInfo.contentSha, this.offset, data]))
            this.offset += data.length

            this.status.shaBytesSent += data.length
        }
        callback(null, null)
    }
}

const GIGABYTE = 1024 * 1024 * 1024

export interface UploadStatus {
    phase: string
    toSync: {
        nbFiles: number
        nbDirectories: number
        nbBytes: number
    }
    visitedFiles: number
    hashedBytes: number
    nbShaSent: number
    shaBytesSent: number
    nbAddedInTx: number // nb files & dirs in tx
    nbBytesInTx: number // equivalent of number of bytes of content actually in the tx
}

export class UploadTransferClient {
    private askShaStatusPayloadsStream: AskShaStatusStream

    status: UploadStatus = {
        phase: "uninitialized",
        toSync: {
            nbFiles: -1,
            nbDirectories: -1,
            nbBytes: -1
        },
        visitedFiles: 0,
        hashedBytes: 0,
        nbShaSent: 0,
        shaBytesSent: 0,
        nbAddedInTx: 0, // nb files & dirs in tx
        nbBytesInTx: 0 // equivalent of number of bytes of content actually in the tx
    }

    private giveStatus() {
        if (this.status.phase == this.ESTIMATING_PHASE_NAME) {
            return [`ESTIMATING WORKLOAD : ${this.status.toSync.nbFiles} files, ${this.status.toSync.nbDirectories} directories, ${(this.status.toSync.nbBytes / GIGABYTE).toFixed(3)} Gb so far`]
        }
        else {
            let totalItems = this.status.toSync.nbFiles >= 0 ? `/${this.status.toSync.nbFiles + this.status.toSync.nbDirectories}` : ''
            let totalBytes = this.status.toSync.nbBytes >= 0 ? `/${(this.status.toSync.nbBytes / GIGABYTE).toFixed(3)}` : ''

            let res = [
                `PUSHING ${this.pushedDirectory}`,
                `listed files         : ${this.status.visitedFiles}${totalItems} files${this.askShaStatusPayloadsStream && this.askShaStatusPayloadsStream.sourceStream ? '' : ', listing finished'}`,
                `pending sha requests : ${this.askShaStatusPayloadsStream.waitedShas.size}`,
                `hashing              : ${(this.status.hashedBytes / GIGABYTE).toFixed(3)}${totalBytes} Gb hashed`,
                `files transferred    : ${this.askShaStatusPayloadsStream.fileStream ? '[IN PROGRESS], ' : ''}${this.status.nbShaSent} files, ${(this.status.shaBytesSent / GIGABYTE).toFixed(3)} Gb`,
                `confirmed in tx      : ${this.status.nbAddedInTx}${totalItems} files, ${(this.status.nbBytesInTx / GIGABYTE).toFixed(3)}${totalBytes} Gb`,
                `phase                : ${this.status.phase}`
            ]

            if (this.status.toSync.nbFiles >= 0)
                res.push(`completed            : ${(100 * this.status.nbBytesInTx / this.status.toSync.nbBytes).toFixed(3)} %`)

            return res
        }
    }

    private ESTIMATING_PHASE_NAME = 'estimating'

    constructor(private pushedDirectory: string, private sourceId: string, private estimateSize: boolean, private socket: Net.Socket) { }

    start() {
        if (this.estimateSize) {
            log(`estimation of work, browsing directories...`)
            this.status.phase = 'estimating target'

            let directoryLister = new DirectoryLister.DirectoryLister(this.pushedDirectory)

            directoryLister.on('end', () => {
                log(`prepared to send ${this.status.toSync.nbDirectories} directories, ${this.status.toSync.nbFiles} files and ${this.status.toSync.nbBytes / (1024 * 1024 * 1024)} Gb`)
                this.startSending()
            })

            this.status.phase = this.ESTIMATING_PHASE_NAME
            directoryLister.on('data', (file: UploadTransferModel.FileInfo) => {
                this.status.toSync.nbDirectories += file.isDirectory ? 1 : 0
                this.status.toSync.nbFiles += file.isDirectory ? 0 : 1
                this.status.toSync.nbBytes += file.size
            })
        }
        else {
            this.startSending()
        }

        log.setStatus(() => this.giveStatus())
    }

    private startSending() {
        this.status.phase = 'sending'

        log(`start sending`)

        let processStream = new Stream.Transform({ objectMode: true })
        processStream._transform = (message, encoding, callback) => {
            let [messageType, content] = Serialization.deserialize(message, null)

            switch (messageType) {
                case UploadTransferModel.MSG_TYPE_REP_BEGIN_TX: {
                    let txId = content

                    log(`starting transaction ${txId}`)

                    let shaCache = new ShaCache.ShaCache(fsPath.join(this.pushedDirectory, '.hb-cache'))
                    let shaProcessor = new ShaProcessor.ShaProcessor(shaCache, hashedBytes => this.status.hashedBytes = hashedBytes)
                    let directoryLister = new DirectoryLister.DirectoryLister(this.pushedDirectory, (nbFiles, nbDirectories) => this.status.visitedFiles = nbFiles + nbDirectories)

                    this.askShaStatusPayloadsStream = new AskShaStatusStream(this.pushedDirectory, this.status)
                    this.askShaStatusPayloadsStream.initSourceStream(directoryLister.pipe(shaProcessor))

                    this.askShaStatusPayloadsStream
                        .pipe(new Socket2Message.MessageToPayloadStream())
                        .pipe(this.socket, { end: false })

                    this.askShaStatusPayloadsStream.on('end', () => {
                        Socket2Message.sendMessageToSocket(Serialization.serialize([UploadTransferModel.MSG_TYPE_COMMIT_TX]), this.socket)
                        log(`upload finished`)
                    })
                    break
                }

                case UploadTransferModel.MSG_TYPE_REP_SHA_STATUS:
                    let [sha, size] = content

                    this.askShaStatusPayloadsStream.receivedShaInformation(sha, size)
                    break

                default:
                    log.err(`received unknown msg type ${messageType}`)
            }

            callback()
        }

        this.socket.on('close', () => {
            log('connection to server closed')
        })

        this.socket.on('error', () => {
            this.socket.end()
            log('connection to server closed')
        })

        this.socket.pipe(new Socket2Message.SocketDataToMessageStream()).pipe(processStream)

        Socket2Message.sendMessageToSocket(Serialization.serialize([UploadTransferModel.MSG_TYPE_ASK_BEGIN_TX, this.sourceId]), this.socket)
    }
}