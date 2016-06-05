import fs = require('fs');
import crypto = require('crypto');
import fsPath = require('path');

const EMPTY_PAYLOAD_SHA = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

async function lstat(path: string) {
    return new Promise<fs.Stats>((resolve, reject) => {
        fs.lstat(path, (err, stats) => {
            if (err) {
                console.log(`error lstat on ${path} : err`);
                reject(err);
            }
            else
                resolve(stats);
        });
    });
}

async function openFile(fileName: string, flags: string) {
    return new Promise<number>((resolve, reject) => {
        fs.open(fileName, flags, (err, fd) => {
            if (err)
                reject(err);
            else
                resolve(fd);
        });
    });
}

async function readFile(fd: number, offset: number, length: number) {
    return new Promise<Buffer>((resolve, reject) => {
        let buffer = new Buffer(length);

        fs.read(fd, buffer, 0, length, offset, (err, bytesRead, buffer) => {
            if (err || bytesRead != length)
                reject(`error reading file`);
            else
                resolve(buffer);
        });
    });
}

async function writeFile(fd: number, data: string) {
    return new Promise<number>((resolve, reject) => {
        fs.write(fd, data, 0, 'utf8', (err, written, buffer) => {
            if (err)
                reject(err);
            else
                resolve(written);
        });
    });
}

async function closeFile(fd: number) {
    return new Promise<void>((resolve, reject) => {
        fs.close(fd, (err) => {
            if (err)
                reject(err);
            else
                resolve();
        })
    });
}

function hashString(value: string) {
    if (value === "")
        return EMPTY_PAYLOAD_SHA;

    let hash = crypto.createHash('sha256');
    hash.update(value);
    return hash.digest('hex');
}

async function hashFile(fileName: string): Promise<string> {
    return new Promise<string>(async (resolve, reject) => {
        let hash = crypto.createHash('sha256');

        try {
            let stat = await lstat(fileName);
            if (stat.size == 0) {
                resolve(EMPTY_PAYLOAD_SHA);
                return;
            }
        }
        catch (error) {
            console.log(`error reading ${fileName}`);
            reject(`error reading ${fileName}`);
            return;
        }

        let input = fs.createReadStream(fileName);

        input.on('data', chunk => {
            hash.update(chunk);
        }).on('end', () => {
            resolve(hash.digest('hex'));
        }).on('error', () => {
            console.log(`error reading ${fileName}`);
            reject(`error reading ${fileName}`);
        });
    });
}

async function readDir(path: string): Promise<string[]> {
    return new Promise<string[]>((resolve, reject) => {
        fs.readdir(path, (err, files) => {
            if (err)
                reject(`error reading directory ${path}`);
            else
                resolve(files);
        });
    });
}

interface FileDescriptor {
    name: string;
    isDirectory: boolean;
    size: number,
    lastWrite: number,
    contentSha: string;
}

class ShaCache {
    private cacheDirectory: string;
    private cache: any;
    private dirtyCache: boolean = false;

    constructor(cacheDirectory: string) {
        this.cacheDirectory = fsPath.resolve(cacheDirectory);
        if (!fs.existsSync(this.cacheDirectory))
            fs.mkdirSync(this.cacheDirectory);

        let cacheFileName = fsPath.join(this.cacheDirectory, 'data');
        if (fs.existsSync(cacheFileName))
            this.cache = JSON.parse(fs.readFileSync(cacheFileName, 'utf8'));
        else
            this.cache = {};
    }

    flushToDisk() {
        if (this.dirtyCache) {
            let cacheFileName = fsPath.join(this.cacheDirectory, 'data');
            fs.writeFileSync(cacheFileName, JSON.stringify(this.cache), 'utf8');
            this.dirtyCache = false;
        }
    }

    async hashFile(fullFileName: string): Promise<string> {
        return new Promise<string>(async (resolve, reject) => {
            if (!fsPath.isAbsolute(fullFileName))
                throw "path should be absolute";

            let stat = fs.lstatSync(fullFileName);

            if (fullFileName in this.cache) {
                let cacheInfo = this.cache[fullFileName];
                if (cacheInfo.lastWrite == stat.mtime.getTime() && cacheInfo.size == stat.size) {
                    resolve(cacheInfo.contentSha);
                    return;
                }
            }

            console.log(`hashing ${fullFileName}...`);

            let contentSha = await hashFile(fullFileName);
            let cacheInfo = {
                lastWrite: stat.mtime.getTime(),
                size: stat.size,
                contentSha: contentSha
            };

            this.cache[fullFileName] = cacheInfo;
            this.dirtyCache = true;

            if (stat.size > 10 * 1024 * 1024)
                this.flushToDisk();

            resolve(contentSha);
        });
    }
}

async function readDirDeep(path: string, shaCache: ShaCache, ignoredNames: string[]): Promise<FileDescriptor[]> {
    return new Promise<FileDescriptor[]>(async (resolve, reject) => {
        let result: FileDescriptor[] = [];

        let stack = [path];
        while (stack.length > 0) {
            let currentPath = stack.pop();
            let files = await readDir(currentPath);

            for (let key in files) {
                let fileName = files[key];
                if (ignoredNames.some(name => fileName == name))
                    continue;

                let fullFileName = fsPath.join(currentPath, fileName);
                let stat = fs.lstatSync(fullFileName);

                let desc = {
                    name: fsPath.relative(path, fullFileName),
                    isDirectory: stat.isDirectory(),
                    lastWrite: stat.mtime.getTime(),
                    contentSha: null,
                    size: 0
                };

                if (stat.isDirectory()) {
                    stack.push(fullFileName);
                }
                else {
                    let sha = await shaCache.hashFile(fullFileName);
                    desc.contentSha = sha;
                    desc.size = stat.size;
                }

                result.push(desc);
            }

            shaCache.flushToDisk();
        }

        resolve(result);
    });
}

interface DirectoryDescriptor {
    files: FileDescriptor[];
}

// READER
// .hb-cache => cache fileName, modif date, sha
// produces the current state
class HexaBackupReader {
    private rootPath: string;
    private shaCache: ShaCache;
    private ignoredNames = ['.hb-cache', '.git', '.metadata'];

    constructor(rootPath: string, private clientId: string) {
        this.rootPath = fsPath.resolve(rootPath);

        let cachePath = fsPath.join(this.rootPath, '.hb-cache');
        this.shaCache = new ShaCache(cachePath);
    }

    async readDirectoryState(): Promise<DirectoryDescriptor> {
        return new Promise<DirectoryDescriptor>(async (resolve, reject) => {
            let result: DirectoryDescriptor = {
                files: []
            };

            result.files = await readDirDeep(this.rootPath, this.shaCache, this.ignoredNames);

            resolve(result);
        });
    }

    async sendSnapshotToStore(store: HexaBackupStore) {
        let transactionId = await store.startOrContinueSnapshotTransaction(this.clientId);

        console.log(`reading directory structure ${this.rootPath}`);
        let desc = await this.readDirectoryState();

        console.log('sending files...');
        for (let fileDesc of desc.files) {
            if (fileDesc.isDirectory) {
                await store.pushFileDescriptor(this.clientId, transactionId, fileDesc);
                continue;
            }

            let fullFileName = fsPath.join(this.rootPath, fileDesc.name);

            let currentSize = await store.hasShaBytes(fileDesc.contentSha);
            let stat = fs.lstatSync(fullFileName);

            if (currentSize < stat.size) {
                const maxBlockSize = 1024 * 1024;

                let fd = await openFile(fullFileName, 'r');

                let currentReadPosition = currentSize;

                while (currentReadPosition < stat.size) {
                    let chunkSize = stat.size - currentReadPosition;
                    if (chunkSize > maxBlockSize)
                        chunkSize = maxBlockSize;

                    if (chunkSize > 0) {
                        let buffer = await readFile(fd, currentReadPosition, chunkSize);

                        await store.putShaBytes(fileDesc.contentSha, currentReadPosition, buffer);

                        currentReadPosition += buffer.length;
                    }
                }

                await closeFile(fd);
            }

            console.log(`push ${fileDesc.name}`);
            await store.pushFileDescriptor(this.clientId, transactionId, fileDesc);
        }

        console.log(`commiting ${this.clientId}::${transactionId}...`);
        await store.commitTransaction(this.clientId, transactionId);

        console.log('snapshot sent');
    }
}

class ReferenceRepository {
    private rootPath: string;

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);
        if (!fs.existsSync(this.rootPath))
            fs.mkdirSync(this.rootPath);
    }

    async put(name: string, value: any) {
        return new Promise<void>(async (resolve, reject) => {
            let contentFileName = this.contentFileName(name);

            if (value == null) {
                fs.unlinkSync(contentFileName);
                resolve();
            }
            else {
                let serializedValue = JSON.stringify(value);

                let fd = await openFile(contentFileName + '.part', 'w');
                await writeFile(fd, serializedValue);
                await closeFile(fd);

                fs.rename(contentFileName + '.part', contentFileName, (err) => {
                    if (err)
                        reject(err);
                    else
                        resolve();
                });
            }
        });
    }

    async get(name: string) {
        return new Promise<any>(async (resolve, reject) => {
            let contentFileName = this.contentFileName(name);
            if (fs.existsSync(contentFileName)) {
                let content = fs.readFileSync(contentFileName, 'utf8');
                try {
                    resolve(JSON.parse(content));
                }
                catch (error) {
                    resolve(null);
                }
            }
            else {
                resolve(null);
            }
        });
    }

    private contentFileName(referenceName: string) {
        return fsPath.join(this.rootPath, `${referenceName.toLocaleUpperCase()}`);
    }
}

class ObjectRepository {
    private rootPath: string;

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);
        if (!fs.existsSync(this.rootPath))
            fs.mkdirSync(this.rootPath);
    }

    async storePayload(payload: string) {
        return new Promise<string>(async (resolve, reject) => {
            let sha = hashString(payload);

            if (await this.hasShaBytes(sha) == 0)
                await this.putShaBytes(sha, 0, new Buffer(payload, 'utf8'));

            resolve(sha);
        });
    }

    async storeObject(object: any) {
        return this.storePayload(JSON.stringify(object));
    }

    async readPayload(sha: string) {
        return new Promise<string>((resolve, reject) => {
            if (sha == EMPTY_PAYLOAD_SHA) {
                resolve('');
                return;
            }

            let contentFileName = this.contentFileName(sha);
            if (!fs.existsSync(contentFileName)) {
                resolve(null);
                return;
            }

            try {
                let content = fs.readFileSync(contentFileName, 'utf8');
                resolve(content);
            } catch (error) {
                reject(error);
            }
        });
    }

    async readObject(sha: string) {
        return await JSON.parse(await this.readPayload(sha));
    }

    /**
     * Returns the number of bytes that are currently committed for the specified sha
     */
    async hasShaBytes(sha: string) {
        return new Promise<number>((resolve, reject) => {
            if (sha == EMPTY_PAYLOAD_SHA) {
                resolve(0);
                return;
            }

            let contentFileName = this.contentFileName(sha);
            if (fs.existsSync(contentFileName))
                resolve(fs.lstatSync(contentFileName).size);
            else
                resolve(0);
        });
    }

    async putShaBytes(sha: string, offset: number, data: Buffer): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            if (sha == EMPTY_PAYLOAD_SHA) {
                resolve();
                return;
            }

            let contentFileName = this.contentFileName(sha);
            fs.open(contentFileName, 'a', (err, fd) => {
                if (err) {
                    reject(err);
                    return;
                }

                fs.write(fd, data, 0, data.byteLength, offset, (err, written, buffer) => {
                    fs.close(fd, (err) => {
                        if (err) {
                            reject(err);
                            return;
                        }

                        resolve();
                    });
                });
            });
        });
    }

    async validateSha(desc: FileDescriptor) {
        return new Promise<boolean>(async (resolve, reject) => {
            if (desc.contentSha == EMPTY_PAYLOAD_SHA) {
                resolve(true);
                return;
            }

            try {
                let contentFileName = this.contentFileName(desc.contentSha);
                let storedContentSha = await hashFile(contentFileName);
                if (storedContentSha == desc.contentSha && desc.size == fs.lstatSync(contentFileName).size) {
                    resolve(true);
                }
                else {
                    fs.unlink(contentFileName, (err) => {
                        resolve(false);
                    });
                }
            } catch (error) {
                console.log(`error validating sha of ${desc.name}`);
                resolve(false);
            }
        });
    }

    private contentFileName(sha: string) {
        let prefix = sha.substring(0, 3);
        let directory = fsPath.join(this.rootPath, prefix);
        if (!fs.existsSync(directory))
            fs.mkdirSync(directory);
        return fsPath.join(this.rootPath, prefix, `${sha}`);
    }
}

interface ClientState {
    currentTransactionId: string;
    currentTransactionContent: { [key: string]: FileDescriptor };
    currentCommitSha: string;
}

interface Commit {
    parentSha: string;
    commitDate: number;
    directoryDescriptorSha: string;
}

class HexaBackupStore {
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
            let clientState: ClientState = await this.getClientState(clientId);
            resolve(clientState.currentTransactionId);
        });
    }

    async hasShaBytes(sha: string) {
        return this.objectRepository.hasShaBytes(sha);
    }

    async putShaBytes(sha: string, offset: number, data: Buffer) {
        return this.objectRepository.putShaBytes(sha, offset, data);
    }

    async pushFileDescriptor(clientId: string, transactionId: string, fileDesc: FileDescriptor) {
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
                let validated = await this.objectRepository.validateSha(fileDesc);
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
                let currentCommit: Commit = await this.objectRepository.readObject(clientState.currentCommitSha);
                if (currentCommit.directoryDescriptorSha == descriptorSha) {
                    console.log(`transaction ${transactionId} makes no change, ignoring`);
                    saveCommit = false;
                }
            }

            // prepare and store the commit
            if (saveCommit) {
                let commit: Commit = {
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

    private async createDirectoryDescriptor(content: { [key: string]: FileDescriptor }) {
        let descriptor: DirectoryDescriptor = {
            files: []
        };

        for (let k in content)
            descriptor.files.push(content[k]);

        return descriptor;
    }

    private lastSeenClientState: { [key: string]: ClientState } = {};

    private async getClientState(clientId: string) {
        return new Promise<ClientState>(async (resolve, reject) => {
            if (this.lastSeenClientState != null && clientId in this.lastSeenClientState) {
                resolve(this.lastSeenClientState[clientId]);
                return;
            }

            let clientStateReferenceName = `client_${clientId}`;
            let clientState: ClientState = await this.referenceRepository.get(clientStateReferenceName);
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

    private async storeClientState(clientId: string, clientState: ClientState, force: boolean) {
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

async function run() {
    console.log("Test load for Hexa-Backup !");

    /*let fileName = 'package.json'; //'d:\\downloads\\crackstation.txt.gz'
    let hex = await hashFile(fileName);
    console.log(`hash: ${hex}`);

    let dirName = '.';
    let files = await readDir(dirName);
    console.log(`files: ${files.join()}`);

    let shaCache = new ShaCache(`d:\\tmp\\.hb-cache`);
    let shaContent = await shaCache.hashFile('d:\\tmp\\CCF26012016.png');
    shaCache.flushToDisk();
    console.log(`picture sha : ${shaContent}`);*/

    //let backupedDirectory = `D:\\Tmp\\Conseils d'Annelise pour la prochaine AG`;
    let backupedDirectory = `D:\\Entreprise`;

    let reader = new HexaBackupReader(backupedDirectory, 'pc-arnaud');
    //let desc = await reader.readDirectoryState();
    //console.log(`descriptor: ${JSON.stringify(desc)}`);

    let store = new HexaBackupStore(`D:\\Tmp\\HexaBackupStore`);

    await reader.sendSnapshotToStore(store);
    console.log('snapshot sent !');
}

run();