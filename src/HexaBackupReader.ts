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
                let fileSize = fs.lstatSync(fullFileName).size

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

        let status = {
            start: Date.now(),
            nbFiles: 0,
            nbDirectories: 0,
            totalBytes: 0,
            transferredBytes: 0,
            transferredFiles: 0,
            currentFile: null
        }

        let workPool = new WorkPool<Model.FileDescriptor>(50, async (batch) => {
            let shas = {}
            batch.forEach((b) => shas[b.contentSha] = b)
            let uniqueShas = []
            for (let i in shas)
                uniqueShas.push(shas[i])

            let currentSizes = await store.hasShaBytes(uniqueShas.map((fileDesc) => fileDesc.contentSha).filter((sha) => sha != null))
            let poolDesc = this.createPoolDescription(uniqueShas, currentSizes)
            let dataStream: NodeJS.ReadableStream = new ShasDataStream(poolDesc, this.rootPath, status, this.gauge())

            if (useZip) {
                let zipped = ZLib.createGzip({
                    memLevel: 9,
                    level: 9
                })
                dataStream = dataStream.pipe(zipped)
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
            log.dbg(`validated ${nbSuccess} files on remote store`)
        })

        let directoryLister = new DirectoryLister(this.rootPath, this.shaCache, this.ignoredNames)

        this.gauge().show(`listing and hashing files...`, 0)

        await directoryLister.readDir(async (fileDesc) => {
            if (fileDesc.isDirectory)
                status.nbDirectories++
            else
                status.nbFiles++

            status.totalBytes += fileDesc.size

            await workPool.addWork(fileDesc)
        })

        await workPool.emptied()

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

        let now = Date.now()
        if (now > lastGauge + 1000) {
            lastGauge = now

            let elapsed = now - this.status.start
            let speed = elapsed > 0 ? this.status.transferredBytes / elapsed : 0
            let s = `${this.status.nbDirectories} directories, ${this.status.transferredFiles}/${this.status.nbFiles} files, ${this.status.transferredBytes}/${this.status.totalBytes} bytes - ${speed.toFixed(2)} kb/s - current file: ${this.status.currentFile ? this.status.currentFile.fileName : '-'}`

            this.gauge.show(s, this.status.transferredBytes / this.status.totalBytes)
        }

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

                this.status.currentFile = fileDesc

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

                    this.status.transferredFiles++
                    this.status.currentFile = null
                }
                else {
                    log.dbg(`read ${length} @ ${this.offset}`)

                    askedIo = true

                    let offset = this.offset

                    this.offset += length
                    this.size -= length

                    this.status.transferredBytes += length

                    let buffer = await FsTools.readFile(this.fd, offset, length)

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
                let stat = fs.lstatSync(fullFileName);

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
                    let sha = await this.shaCache.hashFile(fullFileName);
                    desc.contentSha = sha;
                    desc.size = stat.size;
                }

                await callback(desc);
            }

            this.shaCache.flushToDisk();
        }
    }
}