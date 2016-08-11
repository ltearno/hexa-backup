import fs = require('fs');
import fsPath = require('path');
import * as HashTools from './HashTools';
import * as FsTools from './FsTools';
import * as Stream from 'stream'

const log = require('./Logger')('ObjectRepository');

export interface ShaPoolDescriptor {
    fileName: string;
    sha: string;
    offset: number;
    size: number;
}

export class ObjectRepository {
    private rootPath: string;

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);
        if (!fs.existsSync(this.rootPath))
            fs.mkdirSync(this.rootPath);
    }

    async storePayload(payload: string) {
        return new Promise<string>(async (resolve, reject) => {
            let sha = HashTools.hashString(payload)

            let buffer = new Buffer(payload, 'utf8')

            if (await this.hasOneShaBytes(sha) != buffer.byteLength)
                await this.putShaBytes(sha, 0, buffer);

            resolve(sha);
        });
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

    async putShaBytes(sha: string, offset: number, data: Buffer): Promise<number> {
        return new Promise<number>((resolve, reject) => {
            if (sha == HashTools.EMPTY_PAYLOAD_SHA) {
                resolve(0)
                return
            }

            log.dbg(`put bytes for ${sha} @${offset}, size=${data.byteLength}`)

            let contentFileName = this.contentFileName(sha);
            fs.open(contentFileName, 'a', (err, fd) => {
                if (err) {
                    log.err(`putShaBytes: opening ${contentFileName}, err='${err}'`)
                    reject(err)
                    return
                }

                fs.write(fd, data, 0, data.byteLength, offset, (err, written, buffer) => {
                    fs.close(fd, (err) => {
                        if (err) {
                            log.err(`putShaBytes: closing ${contentFileName}, err='${err}'`)
                            reject(err)
                            return
                        }

                        let totalSize = fs.lstatSync(contentFileName).size
                        if (totalSize != (offset + data.byteLength))
                            log.err(`inconsistent object file size : ${totalSize} != (${offset} + ${data.byteLength})`)

                        resolve(written)
                    });
                });
            });
        });
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

    putShasBytesStream(poolDescriptor: ShaPoolDescriptor[], dataStream: Stream.Readable): Promise<boolean> {
        return new Promise<boolean>((resolve, reject) => {
            poolDescriptor.forEach((e) => e.fileName = this.contentFileName(e.sha))

            let writeStream = new ShaPoolStream(poolDescriptor)

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
        return new Promise<boolean>(async (resolve, reject) => {
            if (contentSha == HashTools.EMPTY_PAYLOAD_SHA) {
                resolve(true);
                return;
            }

            try {
                let contentFileName = this.contentFileName(contentSha)
                let storedContentSha = await HashTools.hashFile(contentFileName)
                let stat = fs.lstatSync(contentFileName)
                if (storedContentSha == contentSha && contentSize == stat.size) {
                    resolve(true)
                }
                else {
                    log.err(`validateSha: content sha (${contentSize} bytes) ${contentSha}, stored sha (${stat.size} bytes) ${storedContentSha}`)

                    //fs.unlink(contentFileName, (err) => resolve(false))
                    fs.rename(contentFileName, contentFileName + '.bak', (err) => resolve(false))
                }
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
}

class ShaPoolStream extends Stream.Writable {
    private desc: ShaPoolDescriptor
    private fd = null
    private offset
    private size

    constructor(private poolDesc: ShaPoolDescriptor[]) {
        super()
    }

    async _write(chunk: Buffer, encoding: string, callback: Function) {
        let offsetInChunk = 0

        while (true) {
            if (this.offset > this.size)
                log.err(`offset writing sha pool ${this.offset} > ${this.size}`)

            if (this.fd && this.offset == this.size) {
                fs.closeSync(this.fd)

                log(`written sha ${this.desc.sha} on disk`)

                this.desc = null
                this.fd = null
                this.offset = 0
                this.size = 0
            }

            if (offsetInChunk >= chunk.length)
                break

            if (!this.fd) {
                let desc = this.poolDesc.shift()

                this.desc = desc
                this.fd = fs.openSync(desc.fileName, 'a')
                this.offset = desc.offset
                this.size = desc.offset + desc.size

                log.dbg(`open for writing pool ${desc.fileName}`)
            }

            let length = chunk.length - offsetInChunk
            if (length > this.size)
                length = this.size

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