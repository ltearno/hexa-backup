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

let Gauge = require('gauge');
let FileSize = require('filesize')

const log = require('./Logger')('HexaBackupReader');

export class HexaBackupReader {
    private rootPath: string;
    private shaCache: ShaCache;
    private ignoredNames = ['.hb-cache', '.git', '.metadata']
    private _gauge: any = null

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

        let status = {
            start: null,
            nbFiles: 0,
            nbDirectories: 0,
            totalBytes: 0,
            transferredBytes: 0,
            dataTransferredBytes: 0,
            networkTransferredBytes: 0,
            transferredFiles: 0,
            lastSentFile: null,

            show: (text) => {
                let now = Date.now()

                if (now > lastGauge + 1000) {
                    lastGauge = now

                    let s = `${status.nbDirectories} directories, ${status.transferredFiles}/${status.nbFiles} files, ${FileSize(status.transferredBytes, { base: 10 })}/${FileSize(status.totalBytes, { base: 10 })}`

                    if (status.start) {
                        let elapsed = now - status.start
                        let dataTranferSpeed = elapsed > 0 ? status.dataTransferredBytes / elapsed : 0
                        let networkTranferSpeed = elapsed > 0 ? status.networkTransferredBytes / elapsed : 0

                        let eta = '-'
                        let rest = (status.totalBytes - status.transferredBytes) / 1000
                        if (dataTranferSpeed > 0 && rest > 0) {
                            let etaSecond = rest / dataTranferSpeed
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

                        s += ` - data speed: ${dataTranferSpeed.toFixed(2)} kb/s - network speed: ${networkTranferSpeed.toFixed(2)} kb/s - ETA: ${eta}`
                    }

                    if (text)
                        s += ' - ' + text

                    if (status.lastSentFile)
                        s += ` - last sent file: ${status.lastSentFile.fileName}`

                    this.gauge().show(s, status.transferredBytes / status.totalBytes)
                }
            }
        }

        let poolWorker = async (batch: Model.FileDescriptor[]) => {
            status.show(`beginning work pool of ${batch.length} items, hashing...`)

            for (let i in batch) {
                let b = batch[i]
                if (!b.isDirectory) {
                    let fileName = fsPath.join(this.rootPath, b.name)

                    status.show(`hashing ${fileName}`)

                    let sha = await this.shaCache.hashFile(fileName)
                    b.contentSha = sha
                }
            }

            status.show(`beginning work pool transfer`)

            let shas = {}
            batch.forEach((b) => shas[b.contentSha] = b)
            let uniqueShas = []
            for (let i in shas)
                uniqueShas.push(shas[i])

            let currentSizes = await store.hasShaBytes(uniqueShas.map((fileDesc) => fileDesc.contentSha).filter((sha) => sha != null))
            let poolDesc = this.createPoolDescription(uniqueShas, currentSizes)
            let dataStream: NodeJS.ReadableStream = new ShasDataStream(poolDesc, this.rootPath, status, this.gauge())

            batch.forEach((i) => status.transferredBytes += currentSizes[i.contentSha] || 0)

            if (useZip) {
                let zipped = ZLib.createGzip({
                    memLevel: 9,
                    level: 9
                })
                dataStream = dataStream.pipe(zipped)
            }

            let backup = dataStream.on
            dataStream.on = (name, callback: Function): NodeJS.ReadableStream => {
                if (name == 'data') {
                    let oldCallback = callback
                    callback = (chunk) => {
                        if (status.start == null)
                            status.start = Date.now()

                        status.networkTransferredBytes += chunk ? chunk.length : 0

                        status.show(`network transfer`)

                        oldCallback(chunk)
                    }
                }
                return backup.apply(dataStream, [name, callback])
            }

            await store.putShasBytesStream(poolDesc, useZip, dataStream)

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

        this.gauge().show(`listing files...`, 0)

        let filesList = []

        let lastNb = 0
        await directoryLister.readDir(async (fileDesc) => {
            if (fileDesc.isDirectory)
                status.nbDirectories++
            else
                status.nbFiles++

            status.totalBytes += fileDesc.size

            filesList.push(fileDesc)
            if (lastNb < status.nbFiles + 100) {
                lastNb = status.nbFiles
                this.gauge().show(`listing files ${status.nbDirectories} directories and ${status.nbFiles} files, total: ${FileSize(status.totalBytes, { base: 10 })}`)
            }
        })

        while (filesList.length > 0) {
            let currentPool = []
            let currentPoolFileSize = 0
            while (filesList.length > 0 && currentPoolFileSize < 1024 * 1024 * 5) {
                let desc = filesList.shift()
                currentPool.push(desc)
                currentPoolFileSize += desc.size
            }

            if (currentPool.length > 0)
                await poolWorker(currentPool)
        }

        log(`commit transaction ${this.clientId}::${transactionId}`);
        await store.commitTransaction(this.clientId, transactionId);

        log('snapshot sent.');
    }

    private hideGauge() {
        if (this._gauge)
            this._gauge.hide()
    }

    private gauge() {
        if (this._gauge == null)
            this._gauge = new Gauge()
        return this._gauge
    }
}

let lastGauge = 0

class ShasDataStream extends Stream.Readable {
    private fd
    private offset
    private size

    constructor(private poolDesc: Model.ShaPoolDescriptor[], private rootPath: string, private status: any, private gauge) {
        super()
    }

    async _read(size: number) {
        let askedIo = false

        this.status.show(`reading pool`)

        while (!askedIo) {
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
                }
                else {
                    log.dbg(`read ${length} @ ${this.offset}`)

                    askedIo = true

                    let offset = this.offset

                    this.offset += length
                    this.size -= length

                    let buffer = await FsTools.readFile(this.fd, offset, length)

                    this.status.dataTransferredBytes += length
                    this.status.transferredBytes += length

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