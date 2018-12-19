import fsPath = require('path')
import { ReferenceRepository } from './ReferenceRepository'
import { ObjectRepository } from './ObjectRepository'
import { ShaCache } from './ShaCache';
import * as Model from './Model'
import * as Stream from 'stream'
import { LoggerBuilder } from '@ltearno/hexa-js'

const log = LoggerBuilder.buildLogger('HexaBackupStore')

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
        this.rootPath = fsPath.resolve(rootPath)

        this.shaCache = new ShaCache(fsPath.join(this.rootPath, '.hb-cache'))

        this.objectRepository = new ObjectRepository(fsPath.join(this.rootPath, '.hb-object'), this.shaCache)

        this.referenceRepository = new ReferenceRepository(fsPath.join(this.rootPath, '.hb-refs'))
    }

    async startOrContinueSnapshotTransaction(sourceId: string): Promise<string> {
        let txId = await this.openTransaction(sourceId)
        log(`source ${sourceId} starts or continues transaction ${txId}`)
        return txId
    }

    async hasShaBytes(shas: string[]) {
        return this.objectRepository.hasShaBytes(shas)
    }

    async hasOneShaBytes(sha: string) {
        return this.objectRepository.hasOneShaBytes(sha)
    }

    async validateShaBytes(sha: string) {
        return this.objectRepository.validateShaBytes(sha)
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

    private transactionTempFilesState: { [key: string]: { firstWrite: boolean, } } = {}

    async pushFileDescriptors(sourceId: string, transactionId: string, descriptors: Model.FileDescriptor[]): Promise<{ [sha: string]: boolean }> {
        if (!descriptors || descriptors.length == 0)
            return {}

        let res: { [sha: string]: boolean } = {}

        log.dbg(`validating ${descriptors.length} descriptors in transaction ${transactionId}`)

        let clientState = await this.getSourceState(sourceId)
        if (clientState.currentTransactionId != transactionId) {
            log.err(`source is pushing with a bad transaction id. Currently know:${clientState.currentTransactionId}, pushed: ${transactionId}`)
            return res
        }

        if (this.transactionTempFilesState[transactionId].firstWrite) {
            await this.shaCache.appendToTemporaryFile(transactionId, '{"files":[')
        }

        for (let fileDesc of descriptors) {
            await this.shaCache.appendToTemporaryFile(transactionId, (this.transactionTempFilesState[transactionId].firstWrite ? '' : ',') + JSON.stringify(fileDesc))
            if (this.transactionTempFilesState[transactionId].firstWrite)
                this.transactionTempFilesState[transactionId].firstWrite = false

            // Note : do not rehash because it should have been done already, but could be possible here to be more safe

            log.dbg(`validated ${fileDesc.name} isDir=${fileDesc.isDirectory}, sha=${fileDesc.contentSha} from '${sourceId}' tx ${transactionId}`)

            res[fileDesc.contentSha] = true
        }

        log.dbg(`validated ${descriptors.length} descriptors in transaction ${transactionId}`)

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
            await this.shaCache.appendToTemporaryFile(transactionId, ']}') // closing the JSON structure
            let transactionStream = await this.shaCache.closeTemporaryFileAndReadAsStream(transactionId)
            let descriptorSha = await this.objectRepository.storeObjectFromStream(transactionStream);

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
            await this.storeClientState(sourceId, clientState, true);
            resolve();
        });
    }

    async getSourceState(sourceId: string) {
        if (this.sourceStateCache != null && sourceId in this.sourceStateCache)
            return this.sourceStateCache[sourceId];

        let clientStateReferenceName = `client_${sourceId}`
        let sourceState: Model.SourceState = await this.referenceRepository.get(clientStateReferenceName)

        if (sourceState == null) {
            sourceState = {
                currentTransactionId: null,
                currentCommitSha: null
            };

            this.sourceStateCache[sourceId] = sourceState;
            //this.referenceRepository.put(clientStateReferenceName, sourceState)
        }
        else {
            // old version had a big data structure here. Prune it to free memory !
            if ("currentTransactionContent" in sourceState)
                delete sourceState["currentTransactionContent"];

            this.sourceStateCache[sourceId] = sourceState;
        }

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

        sourceState.currentTransactionId = this.shaCache.createTemporaryFile();

        this.transactionTempFilesState[sourceState.currentTransactionId] = { firstWrite: true }

        await this.storeClientState(sourceId, sourceState, true);

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