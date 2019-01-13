import * as fs from 'fs'
import * as fsPath from 'path'
import { ShaCache } from './ShaCache'
import * as Stream from 'stream'
import * as ZLib from 'zlib'

import { HashTools, FsTools, LoggerBuilder } from '@ltearno/hexa-js'

const log = LoggerBuilder.buildLogger('ObjectRepository')

export interface ShaPoolDescriptor {
    fileName: string;
    sha: string;
    offset: number;
    size: number;
}

export class ObjectRepository {
    private rootPath: string;

    constructor(rootPath: string, private shaCache: ShaCache) {
        this.rootPath = fsPath.resolve(rootPath);
        if (!fs.existsSync(this.rootPath))
            fs.mkdirSync(this.rootPath);
    }

    getRootPath() {
        return this.rootPath
    }

    async stats() {
        let s = await this.countObjects()

        return {
            rootPath: this.rootPath,
            objectCount: s.count,
            objectSize: s.size
        }
    }

    async storePayload(payload: string): Promise<string> {
        let sha = await HashTools.hashString(payload)

        let buffer = Buffer.from(payload, 'utf8')

        if (await this.hasOneShaBytes(sha) != buffer.byteLength)
            await this.putShaBytes(sha, 0, buffer);

        return sha
    }

    async storeObjectFromStream(stream: Stream.Readable): Promise<string> {
        return new Promise<string>((resolve, reject) => {
            log.dbg(`store object by stream`)

            let contentFileName = this.tempFileName()

            let fileStream = (<any>fs).createWriteStream(contentFileName, { flags: 'wx' })

            stream.pipe(fileStream)

            stream.on('error', (err) => {
                log.err('error receiving stream !')
                fileStream.close()
                reject(err)
            })

            fileStream.on('finish', async () => {
                try {
                    log.dbg(`stream finished !`)
                    fileStream.close()

                    let sha = await HashTools.hashFile(contentFileName)

                    fs.renameSync(contentFileName, this.contentFileName(sha))

                    resolve(sha)
                }
                catch (err) {
                    reject(err)
                }
            })
        })
    }

    async storeObject(object: any) {
        return this.storePayload(JSON.stringify(object));
    }

    async readPayload(sha: string) {
        return new Promise<string>((resolve, reject) => {
            if (sha == HashTools.EMPTY_PAYLOAD_SHA) {
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
    async hasShaBytes(shas: string[]): Promise<{ [sha: string]: number }> {
        let result: { [sha: string]: number } = {}

        for (let k in shas) {
            let sha = shas[k]
            if (sha == null)
                continue

            if (sha == HashTools.EMPTY_PAYLOAD_SHA) {
                result[sha] = 0
            }
            else {
                let contentFileName = this.contentFileName(sha)
                try {
                    let stat = await FsTools.lstat(contentFileName)
                    if (stat == null)
                        result[sha] = 0
                    else
                        result[sha] = stat.size
                } catch (error) {
                    result[sha] = 0
                }
            }

        }

        return result
    }

    async hasOneShaBytes(sha: string) {
        let res = await this.hasShaBytes([sha])
        return res[sha]
    }

    private openedShaFiles = new Map<string, number>()

    async validateShaBytes(sha: string) {
        let openedFile = this.openedShaFiles.get(sha)
        if (openedFile) {
            this.openedShaFiles.delete(sha)
            await FsTools.closeFile(openedFile)

            log.dbg(`closed sha file ${sha}, still ${this.openedShaFiles.size} entries`)
        }

        let contentFileName = this.contentFileName(sha)

        try {
            let storedContentSha = this.shaCache ? await this.shaCache.hashFile(contentFileName) : await HashTools.hashFile(contentFileName)

            if (sha != storedContentSha) {
                log.err(`wrong storage bytes for sha ${sha}`)
                fs.rename(contentFileName, contentFileName + '.bak', (err) => { })
            }

            return sha == storedContentSha
        }
        catch (err) {
            log.err(`error validating sha ${err}`)
            return false
        }
    }

    async putShaBytes(sha: string, offset: number, data: Buffer) {
        if (sha == HashTools.EMPTY_PAYLOAD_SHA)
            return 0

        try {
            log.dbg(`put bytes for ${sha} @${offset}, size=${data.byteLength}`)

            let contentFileName = this.contentFileName(sha)

            let fd = this.openedShaFiles.get(sha)
            if (!fd) {
                fd = await FsTools.openFile(contentFileName, 'w')
                this.openedShaFiles.set(sha, fd)
            }

            await FsTools.writeFileBuffer(fd, offset, data)

            return data.length
        }
        catch (e) {
            log.err(`putShaBytes: error ${e}`)
            throw e
        }
    }

    async readShaBytes(sha: string, offset: number, length: number): Promise<Buffer> {
        return new Promise<Buffer>((resolve, reject) => {
            if (sha == HashTools.EMPTY_PAYLOAD_SHA) {
                resolve(Buffer.alloc(0))
                return
            }

            let contentFileName = this.contentFileName(sha)

            if (length <= 0) {
                if (!fs.existsSync(contentFileName)) {
                    reject(`content file does not exist`)
                    return
                }

                let stat = fs.lstatSync(contentFileName)
                log.dbg(`read length is now ${stat.size}`)
                length = stat.size
            }

            log.dbg(`read bytes for ${sha} @${offset}, size=${length}`)

            fs.open(contentFileName, 'r', (err, fd) => {
                if (err) {
                    log.err(`readShaBytes: opening ${contentFileName}, err='${err}'`)
                    reject(err)
                    return
                }

                let buffer = Buffer.alloc(length)
                fs.read(fd, buffer, 0, length, offset, (err, read, buffer) => {
                    fs.close(fd, (err) => {
                        if (err) {
                            log.err(`readShaBytes: closing ${contentFileName}, err='${err}'`)
                            reject(err)
                            return
                        }

                        resolve(buffer)
                    });
                });
            });
        });
    }

    async validateSha(contentSha: string, contentSize: number) {
        if (contentSha == HashTools.EMPTY_PAYLOAD_SHA)
            return Promise.resolve(true)

        return new Promise<boolean>(async (resolve, reject) => {
            try {
                let contentFileName = this.contentFileName(contentSha)
                let storedContentSha: string
                if (this.shaCache)
                    storedContentSha = await this.shaCache.hashFile(contentFileName)
                else
                    storedContentSha = await HashTools.hashFile(contentFileName)

                fs.stat(contentFileName, (err, stat) => {
                    if (contentSize == stat.size && storedContentSha == contentSha) {
                        resolve(true)
                    }
                    else {
                        log.err(`validateSha: content sha (${contentSize} bytes) ${contentSha}, stored sha (${stat.size} bytes) ${storedContentSha}`)

                        fs.rename(contentFileName, contentFileName + '.bak', (err) => resolve(false))
                    }
                })
            } catch (error) {
                log.err(`validateSha: content sha (${contentSize} bytes) ${contentSha}, error validating: '${error}'`)

                resolve(false)
            }
        });
    }

    async autoComplete(shaStart: string): Promise<string> {
        if (!shaStart || shaStart.length < 2)
            return null

        let prefix = shaStart.substring(0, 2)
        let directory = fsPath.join(this.rootPath, prefix)
        if (!await FsTools.fileExists(directory))
            return null

        let files = (await FsTools.readDir(directory))
            .map(name => name.substring(name.lastIndexOf('/') + 1))
            .filter(name => !name.endsWith('.bak'))
            .filter(name => name.startsWith(shaStart))

        if (files && files.length == 1)
            return files[0]

        return null
    }

    private contentFileName(sha: string) {
        let prefix = sha.substring(0, 2);
        let directory = fsPath.join(this.rootPath, prefix)
        if (!fs.existsSync(directory))
            fs.mkdirSync(directory);
        return fsPath.join(this.rootPath, prefix, `${sha}`);
    }

    private tempFileName() {
        return fsPath.join(this.rootPath, `temp_${Date.now()}`);
    }

    private async countObjects() {
        let result = {
            count: 0,
            size: 0
        }

        let dirs = await FsTools.readDir(this.rootPath)
        for (let dir of dirs) {
            dir = fsPath.join(this.rootPath, dir)
            let files = await FsTools.readDir(dir)
            result.count += files.length
            for (let file of files) {
                try {
                    result.size += (await FsTools.lstat(fsPath.join(dir, file))).size
                }
                catch (err) {
                    log.err(`cannot read object file ${file}`)
                }
            }
        }

        return result
    }
}