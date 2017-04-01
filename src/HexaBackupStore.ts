import fsPath = require('path')
import { ReferenceRepository } from './ReferenceRepository'
import { ObjectRepository } from './ObjectRepository'
import { ShaCache } from './ShaCache';
import * as Model from './Model'
import * as Stream from 'stream'
import * as FS from 'fs'

const log = require('./Logger')('HexaBackupStore')

export interface IHexaBackupStore {
    startOrContinueSnapshotTransaction(sourceId: string): Promise<string>
    hasShaBytes(shas: string[]): Promise<{ [sha: string]: number }>
    hasOneShaBytes(sha: string): Promise<number>
    putShaBytes(sha: string, offset: number, data: Buffer): Promise<number>
    putShaBytesStream(sha: string, offset: number, stream: Stream.Readable): Promise<boolean>
    putShasBytesStream(poolDescriptor: Model.ShaPoolDescriptor[], useZip: boolean, dataStream: NodeJS.ReadableStream): Promise<boolean>
    readShaBytes(sha: string, offset: number, length: number): Promise<Buffer>
    pushFileDescriptors(sourceId: string, transactionId: string, descriptors: Model.FileDescriptor[]): Promise<{ [sha: string]: boolean }>
    commitTransaction(sourceId: string, transactionId: string): Promise<void>
    getSourceState(sourceId: string): Promise<Model.SourceState>
    getCommit(sha: string): Promise<Model.Commit>
    getDirectoryDescriptor(sha: string): Promise<Model.DirectoryDescriptor>
}

export class HexaBackupStore implements IHexaBackupStore {
    private rootPath: string;
    private objectRepository: ObjectRepository;
    private referenceRepository: ReferenceRepository;
    private shaCache: ShaCache;

    private sourceStateCache: { [key: string]: Model.SourceState } = {};
    private lastTimeSavedClientState = 0;

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);

        this.shaCache = null;// new ShaCache(fsPath.join(this.rootPath, '.hb-cache'));

        this.objectRepository = new ObjectRepository(fsPath.join(this.rootPath, '.hb-object'), this.shaCache);

        this.referenceRepository = new ReferenceRepository(fsPath.join(this.rootPath, '.hb-refs'));
    }

    async startOrContinueSnapshotTransaction(sourceId: string): Promise<string> {
        let sourceState: Model.SourceState = await this.getSourceState(sourceId)

        if (sourceState.currentTransactionId == null)
            sourceState.currentTransactionId = await this.openTransaction(sourceId)

        log(`source ${sourceId} starts or continues transaction ${sourceState.currentTransactionId}`)

        return sourceState.currentTransactionId
    }

    async hasShaBytes(shas: string[]) {
        return this.objectRepository.hasShaBytes(shas)
    }

    async hasOneShaBytes(sha: string) {
        return this.objectRepository.hasOneShaBytes(sha)
    }

    async putShaBytes(sha: string, offset: number, data: Buffer) {
        return this.objectRepository.putShaBytes(sha, offset, data);
    }

    async putShaBytesStream(sha: string, offset: number, stream: Stream.Readable) {
        return this.objectRepository.putShaBytesStream(sha, offset, stream)
    }

    async putShasBytesStream(poolDescriptor: Model.ShaPoolDescriptor[], useZip: boolean, dataStream: NodeJS.ReadableStream): Promise<boolean> {
        return this.objectRepository.putShasBytesStream(poolDescriptor, useZip, dataStream)
    }

    async readShaBytes(sha: string, offset: number, length: number): Promise<Buffer> {
        return this.objectRepository.readShaBytes(sha, offset, length)
    }

    async pushFileDescriptors(sourceId: string, transactionId: string, descriptors: Model.FileDescriptor[]): Promise<{ [sha: string]: boolean }> {
        let res: { [sha: string]: boolean } = {}

        log(`validating ${descriptors.length} descriptors in transaction ${transactionId}`)

        for (let d in descriptors) {
            let fileDesc = descriptors[d]

            let clientState = await this.getSourceState(sourceId);
            if (clientState.currentTransactionId != transactionId) {
                log.err(`source is pushing with a bad transaction id !`)
                res[fileDesc.contentSha] = false
                continue
            }

            if (fileDesc.name in clientState.currentTransactionContent) {
                let current = clientState.currentTransactionContent[fileDesc.name];
                if (current != null) {
                    if (current.contentSha == fileDesc.contentSha
                        && current.isDirectory == fileDesc.isDirectory
                        && current.lastWrite == fileDesc.lastWrite
                        && current.name == fileDesc.name
                        && current.size == fileDesc.size) {
                        res[fileDesc.contentSha] = true
                        continue
                    }
                }
            }

            let validated = false;
            if (fileDesc.isDirectory) {
                clientState.currentTransactionContent[fileDesc.name] = fileDesc;
                validated = true;
            }
            else {
                validated = await this.objectRepository.validateSha(fileDesc.contentSha, fileDesc.size);
                if (validated)
                    clientState.currentTransactionContent[fileDesc.name] = fileDesc
                else
                    log.err(`cannot validate sha ${fileDesc.contentSha} for file ${fileDesc.name}`)
            }

            if (validated) {
                await this.storeClientState(sourceId, clientState, false)

                log.dbg(`validated ${fileDesc.name} (${fileDesc.contentSha}) from '${sourceId}'`)

                res[fileDesc.contentSha] = true
                continue
            }
            else {
                res[fileDesc.contentSha] = false
                continue
            }
        }

        log(`validated ${descriptors.length} descriptors in transaction ${transactionId}`)

        return res
    }

    async commitTransaction(sourceId: string, transactionId: string) {
        return new Promise<void>(async (resolve, reject) => {
            // maybe ensure the current transaction is consistent

            let clientState = await this.getSourceState(sourceId);
            if (clientState.currentTransactionId != transactionId) {
                reject('client is commiting with a bad transaction id !');
                return;
            }

            // prepare and store directory descriptor
            let descriptor = await this.createDirectoryDescriptor(clientState.currentTransactionContent);
            let descriptorSha = await this.objectRepository.storeObject(descriptor);

            // check if state changed
            let saveCommit = true;
            if (clientState.currentCommitSha != null) {
                let currentCommit: Model.Commit = await this.objectRepository.readObject(clientState.currentCommitSha);
                if (currentCommit == null) {
                    log.err(`not found commit ${clientState.currentCommitSha} for closing transaction ${transactionId}, create a new commit`)

                    clientState.currentCommitSha = null
                    saveCommit = true
                }
                else if (currentCommit.directoryDescriptorSha == descriptorSha) {
                    log(`transaction ${transactionId} makes no change, ignoring`);
                    saveCommit = false;
                }
            }

            // prepare and store the commit
            if (saveCommit) {
                let commit: Model.Commit = {
                    parentSha: clientState.currentCommitSha,
                    commitDate: Date.now(),
                    directoryDescriptorSha: descriptorSha
                };
                let commitSha = await this.objectRepository.storeObject(commit);

                clientState.currentCommitSha = commitSha;

                log(`source ${sourceId} commited content : ${descriptorSha} in commit ${commitSha}`);
            }

            clientState.currentTransactionId = null;
            clientState.currentTransactionContent = null;
            await this.storeClientState(sourceId, clientState, true);
            resolve();
        });
    }

    async getSourceState(sourceId: string) {
        if (this.sourceStateCache != null && sourceId in this.sourceStateCache)
            return this.sourceStateCache[sourceId];

        let clientStateReferenceName = `client_${sourceId}`;
        let sourceState: Model.SourceState = await this.referenceRepository.get(clientStateReferenceName);
        let save = false;
        if (sourceState == null) {
            sourceState = {
                currentTransactionId: null,
                currentTransactionContent: null,
                currentCommitSha: null
            };
            save = true;
        }

        if (save)
            await this.storeClientState(sourceId, sourceState, true);

        this.sourceStateCache[sourceId] = sourceState;
        return sourceState;
    }

    async getCommit(sha: string): Promise<Model.Commit> {
        return await this.objectRepository.readObject(sha);
    }

    async getDirectoryDescriptor(sha: string): Promise<Model.DirectoryDescriptor> {
        return await this.objectRepository.readObject(sha);
    }

    private async openTransaction(sourceId: string) {
        let sourceState = await this.getSourceState(sourceId);

        if (sourceState.currentTransactionId == null) {
            sourceState.currentTransactionId = `tx_${Date.now()}`;
            sourceState.currentTransactionContent = {};

            await this.storeClientState(sourceId, sourceState, true);
        }

        return sourceState.currentTransactionId;
    }

    private async createDirectoryDescriptor(content: { [key: string]: Model.FileDescriptor }) {
        let descriptor: Model.DirectoryDescriptor = {
            files: []
        };

        for (let k in content)
            descriptor.files.push(content[k]);

        return descriptor;
    }

    private async storeClientState(sourceId: string, sourceState: Model.SourceState, force: boolean) {
        this.sourceStateCache[sourceId] = sourceState;

        return new Promise<void>(async (resolve, reject) => {
            let now = Date.now();
            if (force || (now - this.lastTimeSavedClientState > 2000)) {
                this.lastTimeSavedClientState = now;

                let clientStateReferenceName = `client_${sourceId}`;
                await this.referenceRepository.put(clientStateReferenceName, sourceState);
            }
            resolve();
        });
    }
}