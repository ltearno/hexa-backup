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
const progress = require('progress-stream')

let Gauge = require('gauge');

const log = require('./Logger')('HexaBackupReader');

export class HexaBackupReader {
    private rootPath: string;
    private shaCache: ShaCache;
    private ignoredNames = ['.hb-cache', '.git', '.metadata'];

    constructor(rootPath: string, private clientId: string) {
        this.rootPath = fsPath.resolve(rootPath);

        let cachePath = fsPath.join(this.rootPath, '.hb-cache');
        this.shaCache = new ShaCache(cachePath);
    }

    async sendSnapshotToStore(store: IHexaBackupStore) {
        log(`sending directory snapshot ${this.rootPath}`);

        let transactionId = await store.startOrContinueSnapshotTransaction(this.clientId);

        log.dbg(`beginning transaction ${transactionId}`);

        let workPool = new WorkPool<Model.FileDescriptor>(50, async (batch) => {
            let currentSizes = await store.hasShaBytes(batch.map((fileDesc) => fileDesc.contentSha).filter((sha) => sha != null))

            for (let k in batch) {
                let fileDesc = batch[k]
                try {
                    await this.processFileDesc(store, transactionId, fileDesc, currentSizes)
                }
                catch (error) {
                    log.err(`error reading or pushing ${fileDesc.name} : ${error}`)
                }
            }

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

        await directoryLister.readDir(async (fileDesc) => await workPool.addWork(fileDesc))

        await workPool.emptied()

        log(`commit transaction ${this.clientId}::${transactionId}`);
        await store.commitTransaction(this.clientId, transactionId);

        log('snapshot sent.');
    }

    private async processFileDesc(store: IHexaBackupStore, transactionId, fileDesc: Model.FileDescriptor, currentSizes: { [sha: string]: number }) {
        if (fileDesc.isDirectory)
            return

        let fullFileName = fsPath.join(this.rootPath, fileDesc.name)

        let currentSize = currentSizes[fileDesc.contentSha] || 0
        let stat = fs.lstatSync(fullFileName)
        let sent = 0

        if (currentSize < stat.size) {
            log.dbg(`pushing data file '${fullFileName}', pos=${currentSize}...`)

            let fileStream = (<any>fs).createReadStream(fullFileName, { flags: 'r', start: currentSize, bufferSize: 1 })
            let tranferred = 0;

            //let fileStream = new FileStream(fullFileName, currentSize)

            let str = progress({
                length: stat.size,
                transferred: currentSize,
                time: 1000
            });

            let gauge = null
            let startTime = Date.now()

            str.on('progress', function (progress) {
                if (gauge == null)
                    gauge = new Gauge()
                gauge.show(`${fullFileName} ${fileDesc.name} - ${progress.transferred} of ${stat.size} - ${((progress.transferred - currentSize) / (Date.now() - startTime)).toFixed(2)} kb/s`, progress.transferred / stat.size)
            })

            /*let backup = fileStream.on
            fileStream.on = (name, callback) => {
                if (name == 'data') {
                    let oldCallback = callback
                    callback = (chunk) => {
                        log(`read chunk ${chunk ? chunk.length : 0}, total ${tranferred}`)
                        tranferred += chunk ? chunk.length : 0
                        oldCallback(chunk)
                    }
                }
                backup.apply(fileStream, [name, callback])
            }*/

            let ok = await store.putShaBytesStream(fileDesc.contentSha, currentSize, fileStream.pipe(str))

            if (gauge)
                gauge.hide()

            log.dbg(`done sending file.`)

            currentSizes[fileDesc.contentSha] = fileDesc.size
        }
    }
}

class FileStream extends Stream.Readable {
    private fd

    constructor(private fullFileName: string, private offset: number) {
        super()
    }

    async _read(size: number) {
        if (this.fd == null)
            this.fd = await FsTools.openFile(this.fullFileName, 'r')

        let buffer = await FsTools.readFile(this.fd, this.offset, 65536)

        if (buffer == null || buffer.length == 0) {
            this.push(null)
            await FsTools.closeFile(this.fd)
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