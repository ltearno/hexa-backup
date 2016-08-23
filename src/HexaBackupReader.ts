import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as HashTools from './HashTools';
import { ReferenceRepository } from './ReferenceRepository';
import { ObjectRepository } from './ObjectRepository';
import { ShaCache } from './ShaCache';
import { IHexaBackupStore } from './HexaBackupStore';
import * as Model from './Model';
import { WorkPool } from './WorkPool'
import * as Stream from 'stream'
import * as ZLib from 'zlib'

let FileSize = require('filesize')

const log = require('./Logger')('HexaBackupReader');

class Status {
    start = null;
    transferredBytesForSpeed = 0;
    nbFiles = 0;
    nbDirectories = 0;
    totalBytes = 0;
    logicalTransferredBytes = 0;
    dataTransferredBytes = 0;
    networkTransferredBytes = 0;
    transferredFiles = 0;
    lastSentFile = null;
    text = null;

    statusGiver(): () => { message: string; completed: number; } {
        return () => {
            let now = Date.now()

            let s = `${this.nbDirectories} directories, ${this.transferredFiles}/${this.nbFiles} files, ${FileSize(this.logicalTransferredBytes, { base: 10 })}/${FileSize(this.totalBytes, { base: 10 })}`

            let compression = 1
            if (this.networkTransferredBytes > 0) {
                compression = this.dataTransferredBytes / this.networkTransferredBytes

                s += ` - compression: ${compression.toFixed(2)} (total ${((this.logicalTransferredBytes - this.dataTransferredBytes + this.networkTransferredBytes) / this.totalBytes).toFixed(2)}%)`
            }

            if (this.start) {
                let elapsed = now - this.start
                let networkTranferSpeed = elapsed > 0 ? this.transferredBytesForSpeed / elapsed : 0

                let eta = '-'
                let rest = (this.totalBytes - this.logicalTransferredBytes) / 1000
                if (networkTranferSpeed > 0 && rest > 0) {
                    let etaSecond = rest / networkTranferSpeed
                    etaSecond /= compression
                    eta = `${etaSecond.toFixed(0)} second(s)`
                    if (etaSecond > 60) {
                        let etaMinute = etaSecond / 60
                        eta = `${etaMinute.toFixed(0)} minute(s)`
                        if (etaMinute > 60) {
                            let etaHour = etaMinute / 60
                            eta = `${etaHour.toFixed(2)} hours`
                            if (etaHour > 24) {
                                let etaDay = etaHour / 24
                                eta = `${etaDay.toFixed(2)} days`
                            }
                        }
                    }
                }

                s += ` - network speed: ${networkTranferSpeed.toFixed(2)} kb/s - ETA: ${eta}`
            }

            if (this.text)
                s += ' - ' + this.text

            if (this.lastSentFile)
                s += ` - last seen file: ${this.lastSentFile.fileName}`

            return {
                message: s,
                completed: this.logicalTransferredBytes / this.totalBytes
            }
        }
    }
}

export class HexaBackupReader {
    private rootPath: string;
    private shaCache: ShaCache;
    private ignoredNames = ['.hb-cache', '.hb-object', '.hb-refs', '.git', '.metadata', '.settings', '.idea', 'target', 'node_modules', 'gwt-unitCache', '.ntvs_analysis.dat', '.gradle', 'student_pictures']

    constructor(rootPath: string, private clientId: string) {
        this.rootPath = fsPath.resolve(rootPath);

        let ok = true
        try {
            ok = fs.existsSync(this.rootPath)
        }
        catch (e) {
            log.err(`exception: ${e}`)
        }
        if (!ok) {
            log.err(`the path ${this.rootPath} does not exist !`)
            throw `the path ${this.rootPath} does not exist !`
        }

        let cachePath = fsPath.join(this.rootPath, '.hb-cache');
        this.shaCache = new ShaCache(cachePath);
    }

    private createPoolDescription(batch: Model.FileDescriptor[], currentSizes: { [sha: string]: number }): Model.ShaPoolDescriptor[] {
        let res = [];
        for (let k in batch) {
            let fileDesc = batch[k]
            try {
                if (fileDesc.isDirectory)
                    continue

                let fullFileName = fsPath.join(this.rootPath, fileDesc.name)

                let currentSize = currentSizes[fileDesc.contentSha] || 0
                let fileSize = fs.statSync(fullFileName).size

                if (fileSize > currentSize) {
                    res.push({
                        fileName: fileDesc.name,
                        sha: fileDesc.contentSha,
                        offset: currentSize,
                        size: fileSize - currentSize
                    })
                }
            }
            catch (error) {
                log.err(`error processing ${fileDesc.name} : ${error}`)
            }
        }
        return res
    }

    async sendSnapshotToStore(store: IHexaBackupStore, useZip: boolean) {
        log(`sending directory snapshot ${this.rootPath}`);

        let transactionId = await store.startOrContinueSnapshotTransaction(this.clientId);

        log.dbg(`beginning transaction ${transactionId}`);

        let lastGauge = 0

        let status = new Status()
        log.setStatus(status.statusGiver())

        let poolWorkerNb = 0

        let poolWorker = async (batch: Model.FileDescriptor[]) => {
            poolWorkerNb++

            status.text = `beginning work pool ${poolWorkerNb} of ${batch.length} items, hashing...`

            for (let i in batch) {
                let b = batch[i]
                if (!b.isDirectory) {
                    let fileName = fsPath.join(this.rootPath, b.name)

                    status.text = `hashing ${fileName} (${i} / ${batch.length})`

                    let sha = await this.shaCache.hashFile(fileName)
                    b.contentSha = sha
                }
            }

            status.text = `beginning work pool transfer ${poolWorkerNb} with ${batch.length} items`

            let shas = {}
            batch.forEach((b) => shas[b.contentSha] = b)
            let uniqueShas = []
            for (let i in shas)
                uniqueShas.push(shas[i])

            status.text = `asking current sha states, pool ${poolWorkerNb} with ${batch.length} items`
            let currentSizes = await store.hasShaBytes(uniqueShas.map((fileDesc) => fileDesc.contentSha).filter((sha) => sha != null))
            let poolDesc = this.createPoolDescription(uniqueShas, currentSizes)
            let dataStream: NodeJS.ReadableStream = new ShasDataStream(poolDesc, this.rootPath, status)

            batch.forEach((i) => {
                status.logicalTransferredBytes += currentSizes[i.contentSha] || 0
            })

            if (useZip) {
                let zipped = ZLib.createGzip({
                    memLevel: 9,
                    level: 9
                })
                dataStream = dataStream.pipe(zipped)
            }

            status.transferredBytesForSpeed = 0
            status.start = Date.now()

            let backup = dataStream.on
            dataStream.on = (name, callback: Function): NodeJS.ReadableStream => {
                if (name == 'data') {
                    let oldCallback = callback
                    callback = (chunk) => {
                        status.networkTransferredBytes += chunk ? chunk.length : 0
                        status.transferredBytesForSpeed += chunk ? chunk.length : 0

                        status.text = `network transfer, pool ${poolWorkerNb} of ${batch.length} items`

                        oldCallback(chunk)

                        if (status.transferredBytesForSpeed > 1 * 1024 * 1024) {
                            status.transferredBytesForSpeed = 0
                            status.start = Date.now()
                        }
                    }
                }
                return backup.apply(dataStream, [name, callback])
            }

            status.text = `pushing sha byte streams, pool ${poolWorkerNb} of ${batch.length} items`
            await store.putShasBytesStream(poolDesc, useZip, dataStream)

            status.start = null

            status.text = `pushing file descriptors, pool ${poolWorkerNb} of ${batch.length} items`
            let pushResult = await store.pushFileDescriptors(this.clientId, transactionId, batch)
            let nbError = 0
            let nbSuccess = 0
            for (let sha in pushResult) {
                let success = pushResult[sha]
                if (!success) {
                    log.err(`failed validate sha ${sha} (${batch.filter((b) => b.contentSha == sha).map((b) => b.name).join(', ')})`)
                    nbError++
                }
                else {
                    nbSuccess++
                }
            }

            status.transferredFiles += batch.filter(b => !b.isDirectory).length

            log.dbg(`validated ${nbSuccess} files on remote store`)
        }

        let directoryLister = new DirectoryLister(this.rootPath, this.shaCache, this.ignoredNames)

        status.text = `listing files...`

        let filesList = []

        await directoryLister.readDir(async (fileDesc) => {
            if (fileDesc.isDirectory)
                status.nbDirectories++
            else
                status.nbFiles++

            status.totalBytes += fileDesc.size
            status.lastSentFile = { fileName: fileDesc.name }

            filesList.push(fileDesc)

            status.text = `listing files ${status.nbDirectories} directories and ${status.nbFiles} files, total: ${FileSize(status.totalBytes, { base: 10 })}`
        })

        while (filesList.length > 0) {
            let currentPool = []
            let currentPoolFileSize = 0
            while (filesList.length > 0 && currentPoolFileSize < 1024 * 1024 * 50) {
                let desc = filesList.shift()
                currentPool.push(desc)
                currentPoolFileSize += desc.size
            }

            if (currentPool.length > 0)
                await poolWorker(currentPool)
        }

        status.text = `committing transaction ${this.clientId}::${transactionId}`

        await store.commitTransaction(this.clientId, transactionId)

        log(`committed transaction ${this.clientId}::${transactionId}`)

        log.setStatus(null)

        log('snapshot sent.');
    }
}

let lastGauge = 0

class ShasDataStream extends Stream.Readable {
    private fd
    private offset
    private size

    constructor(private poolDesc: Model.ShaPoolDescriptor[], private rootPath: string, private status: Status) {
        super()
    }

    async _read(size: number) {
        let askedIo = false

        let readden = 0

        while ((!askedIo) && readden < size) {
            if (this.fd == null) {
                if (this.poolDesc.length == 0) {
                    this.push(null)
                    log.dbg(`finished pool`)
                    return
                }

                let fileDesc = this.poolDesc.shift()

                let fileName = fsPath.join(this.rootPath, fileDesc.fileName)

                this.fd = fs.openSync(fileName, 'r')
                this.offset = fileDesc.offset
                this.size = fileDesc.size

                this.status.lastSentFile = fileDesc

                this.status.text = `reading pool, opening file ${fileDesc.fileName}`

                log.dbg(`read ${fileName}`)
            }


            try {
                let length = this.size
                if (length > 65536)
                    length = 65536

                if (length == 0) {
                    log.dbg(`close file`)
                    FsTools.closeFile(this.fd)
                    this.fd = null

                    this.status.lastSentFile = null
                }
                else {
                    askedIo = true

                    let offset = this.offset

                    this.offset += length
                    this.size -= length

                    this.status.text = `reading pool, reading file ${length} @ ${offset}`

                    let buffer = await FsTools.readFile(this.fd, offset, length)

                    readden += buffer.length

                    this.status.dataTransferredBytes += buffer.length
                    this.status.logicalTransferredBytes += buffer.length

                    this.push(buffer)
                }
            }
            catch (e) {
                log.err(`error reading ... error=${e}`)
            }
        }
    }
}

class DirectoryLister {
    constructor(private path: string, private shaCache: ShaCache, private ignoredNames: string[]) {
    }

    async readDir(callback: (fileDesc: Model.FileDescriptor) => Promise<void>) {
        let stack = [this.path];
        while (stack.length > 0) {
            let currentPath = stack.pop();
            let files = await FsTools.readDir(currentPath);

            for (let key in files) {
                let fileName = files[key];
                if (this.ignoredNames.some(name => fileName == name))
                    continue;

                let fullFileName = fsPath.join(currentPath, fileName);
                let stat = fs.statSync(fullFileName);

                let desc = {
                    name: fsPath.relative(this.path, fullFileName),
                    isDirectory: stat.isDirectory(),
                    lastWrite: stat.mtime.getTime(),
                    contentSha: null,
                    size: 0
                };

                if (stat.isDirectory()) {
                    stack.push(fullFileName);
                }
                else {
                    desc.size = stat.size;
                }

                await callback(desc);
            }

            this.shaCache.flushToDisk();
        }
    }
}