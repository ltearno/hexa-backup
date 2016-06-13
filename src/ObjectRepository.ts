import fs = require('fs');
import fsPath = require('path');
import * as HashTools from './HashTools';
import * as FsTools from './FsTools';

const log = require('./Logger')('ObjectRepository');

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