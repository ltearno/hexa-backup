import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as HashTools from './HashTools';
import { ReferenceRepository } from './ReferenceRepository';
import { ObjectRepository } from './ObjectRepository';
import { ShaCache } from './ShaCache';
import { IHexaBackupStore } from './HexaBackupStore';
import * as Model from './Model';

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

        let directoryLister = new DirectoryLister(this.rootPath, this.shaCache, this.ignoredNames);
        await directoryLister.readDir(async (fileDesc) => {
            try {
                await this.processFileDesc(store, transactionId, fileDesc)
            }
            catch (error) {
                log.err(`error reading or pushing ${fileDesc.name} : ${error}`);
            }
        });

        log(`commit transaction ${this.clientId}::${transactionId}...`);
        await store.commitTransaction(this.clientId, transactionId);

        log('snapshot sent.');
    }

    private async processFileDesc(store: IHexaBackupStore, transactionId, fileDesc: Model.FileDescriptor) {
        if (fileDesc.isDirectory) {
            await store.pushFileDescriptor(this.clientId, transactionId, fileDesc);
            return;
        }

        let fullFileName = fsPath.join(this.rootPath, fileDesc.name);

        let currentSize = await store.hasShaBytes(fileDesc.contentSha);
        let stat = fs.lstatSync(fullFileName);

        if (currentSize < stat.size) {
            const maxBlockSize = 1024 * 100;

            log(`sending ${stat.size - currentSize} bytes for file ${fileDesc.name} by chunk of ${maxBlockSize}`);

            let fd = await FsTools.openFile(fullFileName, 'r');

            let currentReadPosition = currentSize;

            let gauge = new Gauge()
            gauge.show(fileDesc.name, currentReadPosition / stat.size)

            while (currentReadPosition < stat.size) {
                let chunkSize = stat.size - currentReadPosition;
                if (chunkSize > maxBlockSize)
                    chunkSize = maxBlockSize;

                if (chunkSize > 0) {
                    let buffer = await FsTools.readFile(fd, currentReadPosition, chunkSize);

                    await store.putShaBytes(fileDesc.contentSha, currentReadPosition, buffer);

                    currentReadPosition += buffer.length;

                    gauge.show(fileDesc.name, currentReadPosition / stat.size)
                }
            }

            gauge.hide();

            await FsTools.closeFile(fd);
        }

        await store.pushFileDescriptor(this.clientId, transactionId, fileDesc);
        log(`pushed ${fileDesc.name}`);
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