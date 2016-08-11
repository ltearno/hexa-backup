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

let Gauge = require('gauge');

const log = require('./Logger')('HexaBackupReader');

export class HexaBackupReader {
    private rootPath: string;
    private shaCache: ShaCache;
    private ignoredNames = ['.hb-cache', '.git', '.metadata']
    private _gauge: any = null

    constructor(rootPath: string, private clientId: string) {
        this.rootPath = fsPath.resolve(rootPath);

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

    async sendSnapshotToStore(store: IHexaBackupStore) {
        log(`sending directory snapshot ${this.rootPath}`);

        let transactionId = await store.startOrContinueSnapshotTransaction(this.clientId);

        log.dbg(`beginning transaction ${transactionId}`);

        let workPool = new WorkPool<Model.FileDescriptor>(50, async (batch) => {
            let currentSizes = await store.hasShaBytes(batch.map((fileDesc) => fileDesc.contentSha).filter((sha) => sha != null))

            let poolDesc = this.createPoolDescription(batch, currentSizes)
            let dataStream = new ShasDataStream(poolDesc, this.rootPath)

            await store.putShasBytesStream(poolDesc, dataStream)

            let pushResult = await store.pushFileDescriptors(this.clientId, transactionId, batch)
            let nbError = 0
            let nbSuccess = 0
            for (let sha in pushResult) {
                let success = pushResult[sha]
                if (!success) {
                    log.err(`failed validate sha ${sha}`)
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

        await directoryLister.readDir(async (fileDesc) => await workPool.addWork(fileDesc))

        await workPool.emptied()

        log(`commit transaction ${this.clientId}::${transactionId}`);
        await store.commitTransaction(this.clientId, transactionId);

        log('snapshot sent.');
    }

    private async processFileDesc(store: IHexaBackupStore, fileDesc: Model.FileDescriptor, currentSizes: { [sha: string]: number }) {
        if (fileDesc.isDirectory)
            return

        let fullFileName = fsPath.join(this.rootPath, fileDesc.name)

        let currentSize = currentSizes[fileDesc.contentSha] || 0
        let fileSize = fs.lstatSync(fullFileName).size
        let sent = 0

        if (currentSize < fileSize) {
            log.dbg(`pushing data file '${fullFileName}', pos=${currentSize}...`)

            //this.gauge().show(`sending ${fileDesc.name}`, 0)

            let fileStream = (<any>fs).createReadStream(fullFileName, { flags: 'r', start: currentSize, bufferSize: 1 })
            let transferred = currentSize

            let startTime = Date.now()

            let backup = fileStream.on
            fileStream.on = (name, callback) => {
                if (name == 'data') {
                    let oldCallback = callback
                    callback = (chunk) => {
                        transferred += chunk ? chunk.length : 0

                        let elapsed = Date.now() - startTime
                        let speed = elapsed > 0 ? (transferred - currentSize) / elapsed : 0
                        this.gauge().show(`${fullFileName} - ${transferred} of ${fileSize} - ${speed.toFixed(2)} kb/s`, transferred / fileSize)

                        oldCallback(chunk)
                    }
                }
                backup.apply(fileStream, [name, callback])
            }

            let ok = await store.putShaBytesStream(fileDesc.contentSha, currentSize, fileStream)

            this.hideGauge()

            log.dbg(`done sending file.`)

            currentSizes[fileDesc.contentSha] = fileDesc.size
        }
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

class ShasDataStream extends Stream.Readable {
    private fd
    private offset

    constructor(private poolDesc: Model.ShaPoolDescriptor[], private rootPath: string) {
        super()
    }

    async _read(size: number) {
        if (this.fd == null) {
            if (this.poolDesc.length == 0) {
                this.push(null)
                return
            }

            let fileDesc = this.poolDesc.shift()

            let fullFileName = fsPath.join(this.rootPath, fileDesc.fileName)
            this.fd = await FsTools.openFile(fullFileName, 'r')
        }

        let buffer = await FsTools.readFile(this.fd, this.offset, 65536)

        if (buffer == null || buffer.length == 0) {
            this.push(null)
            await FsTools.closeFile(this.fd)
            this.fd = null
        }
        else {
            this.push(buffer)
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