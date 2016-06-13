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

        let workPool = new WorkPool(async (batch: Model.FileDescriptor[]) => {
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
            log(`validated ${nbSuccess} files on remote store`)
        }, 50)

        let directoryLister = new DirectoryLister(this.rootPath, this.shaCache, this.ignoredNames);
        await directoryLister.readDir(async (fileDesc) => {
            await workPool.addWork(fileDesc)
        });

        await workPool.emptied()

        log(`commit transaction ${this.clientId}::${transactionId}...`);
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
            const maxBlockSize = 1024 * 128

            let fd = await FsTools.openFile(fullFileName, 'r')

            let currentReadPosition = currentSize

            let gauge = null
            let startTime = Date.now()

            while (currentReadPosition < stat.size) {
                let chunkSize = stat.size - currentReadPosition
                if (chunkSize > maxBlockSize)
                    chunkSize = maxBlockSize

                if (chunkSize > 0) {
                    let buffer = await FsTools.readFile(fd, currentReadPosition, chunkSize)

                    log.dbg(`pushing data file '${fullFileName}', pos=${currentReadPosition}, size=${chunkSize}`)

                    let written = await store.putShaBytes(fileDesc.contentSha, currentReadPosition, buffer)
                    if (written != chunkSize) {
                        log.err(`cannot push data file '${fullFileName}', pos=${currentReadPosition}, size=${chunkSize}`)
                        break
                    }

                    currentReadPosition += buffer.length
                    sent += buffer.length

                    if (currentReadPosition < stat.size) {
                        if (gauge == null)
                            gauge = new Gauge()
                        gauge.show(`${fileDesc.name} - ${currentReadPosition} of ${stat.size} - ${(sent / (Date.now() - startTime)).toFixed(2)} kb/s`, currentReadPosition / stat.size)
                    }
                }
            }

            if (gauge)
                gauge.hide()

            await FsTools.closeFile(fd)

            if (sent > 0)
                log(`sent ${sent} bytes for file ${fileDesc.name}`)

            currentSizes[fileDesc.contentSha] = fileDesc.size
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