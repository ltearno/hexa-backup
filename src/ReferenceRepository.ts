import fs = require('fs')
import fsPath = require('path')
import { FsTools, HashTools } from '@ltearno/hexa-js'
import * as Model from './Model.js'

export class ReferenceRepository {
    private rootPath: string
    private repositoryId: string

    constructor(rootPath: string) {
        this.rootPath = fsPath.resolve(rootPath);
        if (!fs.existsSync(this.rootPath))
            fs.mkdirSync(this.rootPath)

        let configPath = fsPath.join(this.rootPath, 'config')
        if (!fs.existsSync(configPath))
            fs.mkdirSync(configPath)

        let idFilePath = fsPath.join(configPath, 'id')
        if (fs.existsSync(idFilePath)) {
            let content = fs.readFileSync(idFilePath, 'utf8')
            this.repositoryId = JSON.parse(content).uuid
        }

        if (!this.repositoryId) {
            this.repositoryId = HashTools.hashStringSync(`${Math.random()}-${Math.random()}-${Math.random()}-${new Date().getTime()}`)
            fs.writeFileSync(idFilePath, JSON.stringify({ uuid: this.repositoryId }), 'utf8')
        }
    }

    async stats() {
        return {
            nbRefs: (await this.list()).length
        }
    }

    getUuid() {
        return this.repositoryId
    }

    async put(name: string, value: Model.SourceState) {
        return await this.putEx(null, name, value)
    }

    async putEx(directory: string, name: string, value: any) {
        return new Promise<void>(async (resolve, reject) => {
            let contentFileName = this.contentFileName(directory, name);

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

    async get(name: string): Promise<Model.SourceState> {
        return await this.getEx(null, name)
    }

    async  getEx<T>(directory: string, name: string): Promise<T> {
        return new Promise<any>(async (resolve, reject) => {
            let contentFileName = this.contentFileName(directory, name);
            if (fs.existsSync(contentFileName)) {
                let content = fs.readFileSync(contentFileName, 'utf8');
                try {
                    let state: T = JSON.parse(content)

                    resolve(state)
                }
                catch (error) {
                    reject(error)
                }
            }
            else {
                resolve(null)
            }
        });
    }

    async list() {
        return FsTools.readDir(this.rootPath).then(files => files.filter(file => {
            let stat = fs.lstatSync(fsPath.join(this.rootPath, file))
            return stat && stat.isFile()
        }))
    }

    private contentFileName(directory: string, referenceName: string) {
        let path = this.rootPath
        if (directory) {
            path = fsPath.join(path, directory)
            if (!fs.existsSync(path))
                fs.mkdirSync(path)
        }

        return fsPath.join(path, `${referenceName.toLocaleUpperCase()}`);
    }
}