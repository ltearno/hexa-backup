import fs = require('fs');
import fsPath = require('path');
import * as HashTools from './HashTools';

export class ObjectRepository {
    private rootPath: string;

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);
        if (!fs.existsSync(this.rootPath))
            fs.mkdirSync(this.rootPath);
    }

    async storePayload(payload: string) {
        return new Promise<string>(async (resolve, reject) => {
            let sha = HashTools.hashString(payload);

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
    async hasShaBytes(sha: string) {
        return new Promise<number>((resolve, reject) => {
            if (sha == HashTools.EMPTY_PAYLOAD_SHA) {
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
            if (sha == HashTools.EMPTY_PAYLOAD_SHA) {
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

    async validateSha(contentSha: string, contentSize: number) {
        return new Promise<boolean>(async (resolve, reject) => {
            if (contentSha == HashTools.EMPTY_PAYLOAD_SHA) {
                resolve(true);
                return;
            }

            try {
                let contentFileName = this.contentFileName(contentSha);
                let storedContentSha = await HashTools.hashFile(contentFileName);
                if (storedContentSha == contentSha && contentSize == fs.lstatSync(contentFileName).size) {
                    resolve(true);
                }
                else {
                    fs.unlink(contentFileName, (err) => {
                        resolve(false);
                    });
                }
            } catch (error) {
                resolve(false);
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