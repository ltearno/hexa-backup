import fs = require('fs');
import fsPath = require('path');
import { ShaCache } from './ShaCache';
import * as HashTools from './HashTools';
import * as FsTools from './FsTools';
import * as Stream from 'stream'
import * as ZLib from 'zlib'

const log = require('./Logger')('ObjectRepository');

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

    async storePayload(payload: string): Promise<string> {
        let sha = HashTools.hashString(payload)

        let buffer = new Buffer(payload, 'utf8')

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
        let storedContentSha = this.shaCache ? await this.shaCache.hashFile(contentFileName) : await HashTools.hashFile(contentFileName)

        if (sha != storedContentSha) {
            log.err(`wrong storage bytes for sha ${sha}`)
            fs.rename(contentFileName, contentFileName + '.bak', (err) => { })
        }

        return sha == storedContentSha
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

    putShaBytesStream(sha: string, offset: number, stream: Stream.Readable): Promise<boolean> {
        return new Promise<boolean>((resolve, reject) => {
            if (sha == HashTools.EMPTY_PAYLOAD_SHA) {
                resolve(true)
                return
            }

            log.dbg(`put bytes by stream for ${sha} @${offset}`)

            let contentFileName = this.contentFileName(sha)

            let fileStream = (<any>fs).createWriteStream(contentFileName, { flags: 'a', start: offset })

            stream.pipe(fileStream)

            stream.on('error', (err) => {
                log.err('error receiving stream !')
                fileStream.close()
                reject(err)
            })

            fileStream.on('finish', () => {
                log.dbg(`stream finished !`)
                fileStream.close()
                resolve(true)
            })
        })
    }

    putShasBytesStream(poolDescriptor: ShaPoolDescriptor[], useZip: boolean, dataStream: NodeJS.ReadableStream): Promise<boolean> {
        poolDescriptor = poolDescriptor.slice()

        return new Promise<boolean>((resolve, reject) => {
            let writeStream = new ShaPoolStream(poolDescriptor, (sha) => this.contentFileName(sha))

            if (useZip) {
                let unzipped = ZLib.createGunzip()

                dataStream = dataStream.pipe(unzipped)
            }

            dataStream.pipe(writeStream)

            writeStream.on('error', (err) => {
                log.err('error receiving stream !')
                reject(err)
            })

            writeStream.on('finish', () => {
                log.dbg(`stream finished !`)
                resolve(true)
            })
        })
    }

    async readShaBytes(sha: string, offset: number, length: number): Promise<Buffer> {
        return new Promise<Buffer>((resolve, reject) => {
            if (sha == HashTools.EMPTY_PAYLOAD_SHA) {
                resolve(new Buffer(0))
                return
            }

            log.dbg(`read bytes for ${sha} @${offset}, size=${length}`)

            let contentFileName = this.contentFileName(sha)
            fs.open(contentFileName, 'r', (err, fd) => {
                if (err) {
                    log.err(`readShaBytes: opening ${contentFileName}, err='${err}'`)
                    reject(err)
                    return
                }

                let buffer = new Buffer(length)
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

    private contentFileName(sha: string) {
        let prefix = sha.substring(0, 2);
        let directory = fsPath.join(this.rootPath, prefix);
        if (!fs.existsSync(directory))
            fs.mkdirSync(directory);
        return fsPath.join(this.rootPath, prefix, `${sha}`);
    }

    private tempFileName() {
        return fsPath.join(this.rootPath, `temp_${Date.now()}`);
    }
}

class ShaPoolStream extends Stream.Writable {
    private desc: ShaPoolDescriptor
    private fd = null
    private offset
    private size

    constructor(private poolDesc: ShaPoolDescriptor[], private fileNamer: (sha: string) => string) {
        super()
    }

    async _write(chunk: Buffer, encoding: string, callback: Function) {
        let offsetInChunk = 0

        while (true) {
            if (this.offset > this.size)
                log.err(`offset writing sha pool ${this.offset} > ${this.size}`)

            if (this.fd && this.offset == this.size) {
                fs.close(this.fd)

                log(`written sha ${this.desc.sha} on disk, size: ${this.size}`)

                this.desc = null
                this.fd = null
                this.offset = 0
                this.size = 0
            }

            if (offsetInChunk >= chunk.length)
                break

            if (!this.fd) {
                let desc = this.poolDesc.shift()

                let fileName = this.fileNamer(desc.sha)

                this.desc = desc
                this.fd = fs.openSync(fileName, 'a')
                this.offset = desc.offset
                this.size = desc.offset + desc.size

                log(`writing sha ${desc.sha}, beginning @ ${this.offset}, size = ${this.size}`)
            }

            let length = chunk.length - offsetInChunk
            if (length > this.size - this.offset)
                length = this.size - this.offset

            await this.writeFile(this.fd, chunk, offsetInChunk, length, this.offset)
            offsetInChunk += length
            this.offset += length
        }

        callback()
    }

    private async writeFile(fd, buffer: Buffer, offsetInSource: number, length, offsetInFile): Promise<number> {
        return new Promise<number>((resolver, rejecter) => {
            fs.write(fd, buffer, offsetInSource, length, offsetInFile, (err, written, buffer) => {
                if (err)
                    rejecter(err)
                else
                    resolver(written)
            })
        })
    }
}