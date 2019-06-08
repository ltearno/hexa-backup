import fs = require('fs')
import fsPath = require('path')
import { FsTools } from '@ltearno/hexa-js'

export class ReferenceRepository {
    private rootPath: string;

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);
        if (!fs.existsSync(this.rootPath))
            fs.mkdirSync(this.rootPath);
    }

    async stats() {
        return {
            nbRefs: (await this.list()).length
        }
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

                let fd = await FsTools.openFile(contentFileName + '.part', 'w');
                await FsTools.writeFile(fd, serializedValue);
                await FsTools.closeFile(fd);

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

    async list() {
        return FsTools.readDir(this.rootPath).then(files => files.filter(file => {
            let stat = fs.lstatSync(fsPath.join(this.rootPath, file))
            return stat && stat.isFile()
        }))
    }

    private contentFileName(referenceName: string) {
        return fsPath.join(this.rootPath, `${referenceName.toLocaleUpperCase()}`);
    }
}