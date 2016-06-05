import fs = require('fs');
import crypto = require('crypto');
import fsPath = require('path');

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
}

// object repository
// reference repository

async function run() {
    console.log("Test load for Hexa-Backup !");

    let fileName = 'package.json'; //'d:\\downloads\\crackstation.txt.gz'
    let hex = await hashFile(fileName);
    console.log(`hash: ${hex}`);

    let dirName = '.';
    let files = await readDir(dirName);
    console.log(`files: ${files.join()}`);

    //let desc = await analyseDirectory(dirName);
    //console.log(`descriptor: ${JSON.stringify(desc)}`);

    let shaCache = new ShaCache(`d:\\tmp\\.hb-cache`);
    let shaContent = await shaCache.hashFile('d:\\tmp\\CCF26012016.png');
    shaCache.flushToDisk();
    console.log(`picture sha : ${shaContent}`);

    let reader = new HexaBackupReader('d:\\tmp');
    let desc = await reader.readDirectoryState();
    console.log(`descriptor: ${JSON.stringify(desc)}`);
}

run();