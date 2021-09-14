import fsPath = require('path')
import { ReferenceRepository } from './ReferenceRepository'
import { ObjectRepository } from './ObjectRepository'
import { ShaCache } from './ShaCache';
import * as Model from './Model'
import { LoggerBuilder } from '@ltearno/hexa-js'

const log = LoggerBuilder.buildLogger('HexaBackupStore')

export interface IHexaBackupStore {
    getRefs(): Promise<string[]>
    getSources(): Promise<string[]>
    hasShaBytes(shas: string[]): Promise<{ [sha: string]: number }>
    hasOneShaBytes(sha: string): Promise<number>
    putShaBytes(sha: string, offset: number, data: Buffer): Promise<number>
    readShaBytes(sha: string, offset: number, length: number): Promise<Buffer>
    registerNewCommit(sourceId: string, directoryDescriptorSha: string): Promise<string>
    setClientState(sourceId: string, state: Model.SourceState): Promise<void>
    getSourceState(sourceId: string): Promise<Model.SourceState>
    getCommit(sha: string): Promise<Model.Commit>
    getDirectoryDescriptor(sha: string): Promise<Model.DirectoryDescriptor>
    validateShaBytes(sha: string): Promise<boolean>
    autoCompleteSha(shaStart: string): Promise<string>
    stats(): Promise<any>
}

type CommitListener = (commitSha: string, clientId: string) => any

export class HexaBackupStore implements IHexaBackupStore {
    private rootPath: string;
    private objectRepository: ObjectRepository;
    private referenceRepository: ReferenceRepository;
    private shaCache: ShaCache;

    private commitListeners: CommitListener[] = []

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath)

        this.shaCache = new ShaCache(fsPath.join(this.rootPath, '.hb-cache'))

        this.objectRepository = new ObjectRepository(fsPath.join(this.rootPath, '.hb-object'), this.shaCache)

        this.referenceRepository = new ReferenceRepository(fsPath.join(this.rootPath, '.hb-refs'))
    }

    /** Public local interface */

    getUuid() {
        return this.referenceRepository.getUuid()
    }

    getObjectRepository() {
        return this.objectRepository
    }

    getReferenceRepository() {
        return this.referenceRepository
    }

    readShaAsStream(sha: string, start: number, end: number) {
        return this.objectRepository.readShaAsStream(sha, start, end)
    }

    getShaFileName(sha: string) {
        return this.objectRepository.getShaFileName(sha)
    }

    addCommitListener(listener: CommitListener) {
        this.commitListeners.push(listener)
    }

    /** Public remote interface */

    async getRefs() {
        return this.referenceRepository.list()
    }

    async getSources() {
        return (await this.getRefs())
            .filter(ref => ref.startsWith('CLIENT_'))
            .map(ref => ref.substr('CLIENT_'.length))
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
        return this.objectRepository.putShaBytes(sha, offset, data)
    }

    async readShaBytes(sha: string, offset: number, length: number): Promise<Buffer> {
        return this.objectRepository.readShaBytes(sha, offset, length)
    }

    async registerNewCommit(sourceId: string, directoryDescriptorSha: string): Promise<string> {
        let clientState = await this.getSourceState(sourceId)
        if (clientState && clientState.readOnly) {
            log.wrn(`refused a new commit on source ${sourceId} because the source is read-only (wanted desc ${directoryDescriptorSha})`)
            return clientState.currentCommitSha
        }

        let saveCommit = true

        // check if state changed
        if (clientState.currentCommitSha != null) {
            let currentCommit: Model.Commit = await this.objectRepository.readObject(clientState.currentCommitSha)
            if (currentCommit == null) {
                log.err(`not found commit ${clientState.currentCommitSha}, create a new commit`)

                clientState.currentCommitSha = null
            }
            else if (currentCommit.directoryDescriptorSha == directoryDescriptorSha) {
                log(`commit introduces no change, ignoring`)
                saveCommit = false
            }
        }

        if (saveCommit) {
            let commit: Model.Commit = {
                parentSha: clientState.currentCommitSha,
                commitDate: Date.now(),
                directoryDescriptorSha
            }

            let commitSha = await this.objectRepository.storeObject(commit)

            clientState.currentCommitSha = commitSha

            log(`source ${sourceId} commited content : ${directoryDescriptorSha} in commit ${commitSha}`)

            this.commitListeners.forEach(listener => listener(commitSha, sourceId))
        }

        await this.storeClientState(sourceId, clientState)

        return clientState.currentCommitSha
    }

    async setClientState(sourceId: string, state: Model.SourceState) {
        await this.storeClientState(sourceId, state)
    }

    async getSourceState(sourceId: string) {
        if (!sourceId)
            return null

        sourceId = sourceId.toLocaleUpperCase()

        let clientStateReferenceName = `client_${sourceId}`
        let sourceState: Model.SourceState = await this.referenceRepository.get(clientStateReferenceName)

        if (!sourceState) {
            return null
        }

        // retrocompatibility with very old sources
        if (sourceState && ("currentTransactionContent" in sourceState)) {
            log(`removing old "currentTransactionContent" field from source state`)
            delete sourceState["currentTransactionContent"]
            await this.referenceRepository.put(clientStateReferenceName, sourceState)
        }

        return sourceState;
    }

    async getCommit(sha: string): Promise<Model.Commit> {
        return await this.objectRepository.readObject(sha);
    }

    async getDirectoryDescriptor(sha: string): Promise<Model.DirectoryDescriptor> {
        return await this.objectRepository.readObject(sha)
    }

    async autoCompleteSha(shaStart: string) {
        return this.objectRepository.autoComplete(shaStart)
    }

    async stats() {
        return {
            rootPath: this.rootPath,
            objectRepository: await this.objectRepository.stats(),
            shaCache: await this.shaCache.stats(),
            referenceRepository: await this.referenceRepository.stats()
        }
    }

    private async storeClientState(sourceId: string, sourceState: Model.SourceState): Promise<boolean> {
        if (!sourceId)
            return false

        sourceId = sourceId.toLocaleUpperCase()

        let clientStateReferenceName = `client_${sourceId}`

        let existingSource = await this.referenceRepository.get(clientStateReferenceName)
        if (existingSource && existingSource.readOnly) {
            return false
        }

        await this.referenceRepository.put(clientStateReferenceName, sourceState)

        return true
    }
}