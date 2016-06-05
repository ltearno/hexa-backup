import fs = require('fs');
import crypto = require('crypto');
import fsPath = require('path');


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
    let hash = crypto.createHash('sha256');
    hash.update(value);
    return hash.digest('hex');
}

async function hashFile(fileName: string): Promise<string> {
    return new Promise<string>((resolve, reject) => {
        let hash = crypto.createHash('sha256');
        let input = fs.createReadStream(fileName);

        input.on('data', chunk => {
            hash.update(chunk);
        }).on('end', () => {
            resolve(hash.digest('hex'));
        }).on('error', () => {
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
                if (cacheInfo.lastWrite == stat.mtime.getTime()) {
                    resolve(cacheInfo.contentSha);
                    return;
                }
            }

            console.log(`hashing ${fullFileName}...`);

            let contentSha = await hashFile(fullFileName);
            let cacheInfo = {
                lastWrite: stat.mtime.getTime(),
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
                    contentSha: null
                };

                if (stat.isDirectory()) {
                    stack.push(fullFileName);
                }
                else {
                    let sha = await shaCache.hashFile(fullFileName);
                    desc.contentSha = sha;
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
    private ignoredNames = ['.hb-cache', '.git'];

    constructor(rootPath: string) {
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
        let desc = await this.readDirectoryState();

        for (let fileDesc of desc.files) {
            if (fileDesc.isDirectory)
                continue;

            let fullFileName = fsPath.join(this.rootPath, fileDesc.name);

            let currentSize = await store.hasShaBytes(fileDesc.contentSha);
            let stat = fs.lstatSync(fullFileName);

            if (currentSize < stat.size) {
                console.log(`need to send ${stat.size - currentSize} bytes for file ${fileDesc.name}`);

                const maxBlockSize = 4096;

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

                let validated = await store.validateSha(fileDesc.contentSha);
                if (!validated) {
                    console.log(`error uploading ${fileDesc.name}, file content might have changed during uploading the content...`);
                }
            }

            // push file descriptor to store
            // store checks that SHA is valid after the transfer => file state is committed in a consistent way (name, modifDate, contentSha)
        }
    }
}

class ObjectRepository {
    private rootPath: string;

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);
        if (!fs.existsSync(this.rootPath))
            fs.mkdirSync(this.rootPath);
    }

    /**
     * Returns the number of bytes that are currently committed for the specified sha
     */
    async hasShaBytes(sha: string) {
        return new Promise<number>((resolve, reject) => {
            let contentFileName = this.contentFileName(sha);
            if (fs.existsSync(contentFileName))
                resolve(fs.lstatSync(contentFileName).size);
            else
                resolve(0);
        });
    }

    async putShaBytes(sha: string, offset: number, data: Buffer): Promise<void> {
        return new Promise<void>((resolve, reject) => {
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

    async validateSha(sha: string) {
        return new Promise<boolean>(async (resolve, reject) => {
            let contentFileName = this.contentFileName(sha);
            let storedContentSha = await hashFile(contentFileName);
            if (storedContentSha == sha) {
                resolve(true);
            }
            else {
                fs.unlink(contentFileName, (err) => {
                    resolve(false);
                });
            }
        });
    }

    private contentFileName(sha: string) {
        return fsPath.join(this.rootPath, `${sha}`);
    }
}

class HexaBackupStore {
    private rootPath: string;
    private objectRepository: ObjectRepository;

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);

        this.objectRepository = new ObjectRepository(fsPath.join(this.rootPath, '.hb-object'));
    }

    startSnapshotTransaction(): string {
        // create space for storing the transaction
        return 'id_tx';
    }

    async hasShaBytes(sha: string) {
        return this.objectRepository.hasShaBytes(sha);
    }

    async putShaBytes(sha: string, offset: number, data: Buffer) {
        return this.objectRepository.putShaBytes(sha, offset, data);
    }

    async validateSha(sha: string) {
        return this.objectRepository.validateSha(sha);
    }

    commitTransaction(idTransaction: string) {
        // ensure state is consistent
        // write state info
    }
}

async function run() {
    console.log("Test load for Hexa-Backup !");

    let fileName = 'package.json'; //'d:\\downloads\\crackstation.txt.gz'
    let hex = await hashFile(fileName);
    console.log(`hash: ${hex}`);

    let dirName = '.';
    let files = await readDir(dirName);
    console.log(`files: ${files.join()}`);

    let shaCache = new ShaCache(`d:\\tmp\\.hb-cache`);
    let shaContent = await shaCache.hashFile('d:\\tmp\\CCF26012016.png');
    shaCache.flushToDisk();
    console.log(`picture sha : ${shaContent}`);

    let backupedDirectory = `D:\\Tmp\\Conseils d'Annelise pour la prochaine AG`;

    let reader = new HexaBackupReader(backupedDirectory);
    let desc = await reader.readDirectoryState();
    console.log(`descriptor: ${JSON.stringify(desc)}`);

    let store = new HexaBackupStore(`D:\\Tmp\\HexaBackupStore`);

    await reader.sendSnapshotToStore(store);
    console.log('snapshot sent !');
}

run();