import fsPath = require('path');
import { ReferenceRepository } from './ReferenceRepository';
import { ObjectRepository } from './ObjectRepository';
import * as Model from './Model';

export class HexaBackupStore {
    private rootPath: string;
    private objectRepository: ObjectRepository;
    private referenceRepository: ReferenceRepository;

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);

        this.objectRepository = new ObjectRepository(fsPath.join(this.rootPath, '.hb-object'));

        this.referenceRepository = new ReferenceRepository(fsPath.join(this.rootPath, '.hb-refs'));
    }

    async startOrContinueSnapshotTransaction(clientId: string) {
        return new Promise<string>(async (resolve, reject) => {
            let clientState: Model.ClientState = await this.getClientState(clientId);
            resolve(clientState.currentTransactionId);
        });
    }

    async hasShaBytes(sha: string) {
        return this.objectRepository.hasShaBytes(sha);
    }

    async putShaBytes(sha: string, offset: number, data: Buffer) {
        return this.objectRepository.putShaBytes(sha, offset, data);
    }

    async pushFileDescriptor(clientId: string, transactionId: string, fileDesc: Model.FileDescriptor) {
        return new Promise<void>(async (resolve, reject) => {
            let clientState = await this.getClientState(clientId);
            if (clientState.currentTransactionId != transactionId) {
                console.log(`client is pushing with a bad transaction id !`);
                return;
            }

            if (fileDesc.name in clientState.currentTransactionContent) {
                let current = clientState.currentTransactionContent[fileDesc.name];
                if (current != null) {
                    if (current.contentSha == fileDesc.contentSha
                        && current.isDirectory == fileDesc.isDirectory
                        && current.lastWrite == fileDesc.lastWrite
                        && current.name == fileDesc.name
                        && current.size == fileDesc.size) {
                        resolve();
                        return;
                    }
                }
            }

            if (fileDesc.isDirectory) {
                clientState.currentTransactionContent[fileDesc.name] = fileDesc;
            }
            else {
                let validated = await this.objectRepository.validateSha(fileDesc.contentSha, fileDesc.size);
                if (validated) {
                    clientState.currentTransactionContent[fileDesc.name] = fileDesc;
                }
            }

            await this.storeClientState(clientId, clientState, false);

            resolve();
        });
    }

    async commitTransaction(clientId: string, transactionId: string) {
        return new Promise<void>(async (resolve, reject) => {
            // maybe ensure the current transaction is consistent

            let clientState = await this.getClientState(clientId);
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
                if (currentCommit.directoryDescriptorSha == descriptorSha) {
                    console.log(`transaction ${transactionId} makes no change, ignoring`);
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

                console.log(`commited content : ${descriptorSha} in commit ${commitSha}`);
            }

            clientState.currentTransactionId = null;
            clientState.currentTransactionContent = null;
            await this.storeClientState(clientId, clientState, true);
            resolve();
        });
    }

    private async createDirectoryDescriptor(content: { [key: string]: Model.FileDescriptor }) {
        let descriptor: Model.DirectoryDescriptor = {
            files: []
        };

        for (let k in content)
            descriptor.files.push(content[k]);

        return descriptor;
    }

    private lastSeenClientState: { [key: string]: Model.ClientState } = {};

    private async getClientState(clientId: string) {
        return new Promise<Model.ClientState>(async (resolve, reject) => {
            if (this.lastSeenClientState != null && clientId in this.lastSeenClientState) {
                resolve(this.lastSeenClientState[clientId]);
                return;
            }

            let clientStateReferenceName = `client_${clientId}`;
            let clientState: Model.ClientState = await this.referenceRepository.get(clientStateReferenceName);
            let save = false;
            if (clientState == null) {
                clientState = {
                    currentTransactionId: null,
                    currentTransactionContent: null,
                    currentCommitSha: null
                };
                save = true;
            }

            if (clientState.currentTransactionId == null) {
                clientState.currentTransactionId = `tx_${Date.now()}`;
                clientState.currentTransactionContent = {};
                save = true;
            }

            if (save)
                await this.storeClientState(clientId, clientState, true);

            this.lastSeenClientState[clientId] = clientState;
            resolve(clientState);
        });
    }

    private lastTimeSavedClientState = 0;

    private async storeClientState(clientId: string, clientState: Model.ClientState, force: boolean) {
        this.lastSeenClientState[clientId] = clientState;

        return new Promise<void>(async (resolve, reject) => {
            let now = Date.now();
            if (force || (now - this.lastTimeSavedClientState > 2000)) {
                this.lastTimeSavedClientState = now;

                let clientStateReferenceName = `client_${clientId}`;
                await this.referenceRepository.put(clientStateReferenceName, clientState);
            }
            resolve();
        });
    }
}