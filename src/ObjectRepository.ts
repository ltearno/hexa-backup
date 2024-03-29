import * as fs from 'fs'
import * as fsPath from 'path'
import { ShaCache } from './ShaCache.js'
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
    private rootPath: string
    private openedShaFiles = new Map<string, number>()

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
            await this.putShaBytes(sha, 0, buffer)

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

                    fs.renameSync(contentFileName, await this.contentFileName(sha))

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
        if (!sha)
            return null

        if (sha == HashTools.EMPTY_PAYLOAD_SHA) {
            return ''
        }

        let contentFileName = this.contentFileNameSync(sha)
        if (!fs.existsSync(contentFileName)) {
            return null
        }

        let content = fs.readFileSync(contentFileName, 'utf8')
        return content
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
                let contentFileName = await this.contentFileName(sha)
                if (!await FsTools.fileExists(contentFileName)) {
                    result[sha] = 0
                }
                else {
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

        }

        return result
    }

    async hasOneShaBytes(sha: string) {
        let res = await this.hasShaBytes([sha])
        return res[sha]
    }

    async validateShaBytes(sha: string) {
        if (!sha)
            return null

        if (sha == HashTools.EMPTY_PAYLOAD_SHA)
            return true

        // backuped.txt in ARNAUD-LAPTOP-DOCUMENTS
        if (sha == "f94eda6227ead95f74281004dd78922bb8022af74ace0a172131c68419554ddc")
            return true

        let openedFile = this.openedShaFiles.get(sha)
        if (openedFile) {
            this.openedShaFiles.delete(sha)
            await FsTools.closeFile(openedFile)

            log.dbg(`closed sha file ${sha}, still ${this.openedShaFiles.size} entries`)
        }

        let contentFileName = await this.contentFileName(sha)

        try {
            let storedContentSha = this.shaCache ? await this.shaCache.hashFile(contentFileName) : await HashTools.hashFile(contentFileName)
            if (!storedContentSha) {
                log.dbg(`cannot hash file for validation (sha=${sha})`)
                return false
            }

            if (sha != storedContentSha) {
                log.err(`wrong storage bytes for sha ${sha}`)
                try {
                    fs.renameSync(contentFileName, contentFileName + '.bak')
                }
                catch (err) {
                    log.err(`error when renaming to .bak file: ${err}`)
                }
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

            let fd = this.openedShaFiles.get(sha)
            if (!fd) {
                let contentFileName = await this.contentFileName(sha)
                fd = await FsTools.openFile(contentFileName, 'w')
                this.openedShaFiles.set(sha, fd)

                log.dbg(`onOpen, ${this.openedShaFiles.size} entries in this.openedShaFiles`)
            }

            await FsTools.writeFileBuffer(fd, offset, data)

            return data.length
        }
        catch (e) {
            log.err(`putShaBytes: error ${e}`)
            throw e
        }
    }

    getShaFileName(sha: string) {
        if (!sha)
            return null

        return this.contentFileNameSync(sha)
    }

    readShaAsStream(sha: string, start: number, end: number) {
        if (!sha)
            return null

        let contentFileName = this.contentFileNameSync(sha)

        if (!fs.existsSync(contentFileName))
            return null

        let options: any = {
            autoClose: true
        }

        if (start >= 0)
            options.start = start
        if (end >= 0)
            options.end = end

        return fs.createReadStream(contentFileName, options)
    }

    async readShaBytes(sha: string, offset: number, length: number): Promise<Buffer> {
        if (sha == HashTools.EMPTY_PAYLOAD_SHA)
            return Buffer.alloc(0)

        let contentFileName = await this.contentFileName(sha)

        if (!fs.existsSync(contentFileName)) {
            throw `content file does not exist`
        }

        if (length <= 0) {
            let stat = fs.lstatSync(contentFileName)
            log.dbg(`read length is now ${stat.size}`)
            length = stat.size
        }

        log.dbg(`read bytes for ${sha} @${offset}, size=${length}`)

        let fd = await FsTools.openFile(contentFileName, 'r')

        let buffer = await FsTools.readFile(fd, offset, length)

        await FsTools.closeFile(fd)

        return buffer
    }

    async validateSha(contentSha: string, contentSize: number) {
        if (contentSha == HashTools.EMPTY_PAYLOAD_SHA)
            return true

        try {
            let contentFileName = await this.contentFileName(contentSha)
            let storedContentSha: string
            if (this.shaCache)
                storedContentSha = await this.shaCache.hashFile(contentFileName)
            else
                storedContentSha = await HashTools.hashFile(contentFileName)

            let stat = await FsTools.stat(contentFileName)

            if (contentSize == stat.size && storedContentSha == contentSha) {
                return true
            }
            else {
                log.err(`validateSha: content sha (${contentSize} bytes) ${contentSha}, stored sha (${stat.size} bytes) ${storedContentSha}`)

                try {
                    fs.renameSync(contentFileName, contentFileName + '.bak')
                }
                catch (err) {
                    log.err(`error when renaming to .bak file in validateSha(...): ${err}`)
                }

                return false
            }
        } catch (error) {
            log.err(`validateSha: content sha (${contentSize} bytes) ${contentSha}, error validating: '${error}'`)

            return false
        }
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

    private contentFileNameSync(sha: string) {
        let prefix = sha.substring(0, 2);
        let directory = fsPath.join(this.rootPath, prefix)
        if (!fs.existsSync(directory))
            fs.mkdirSync(directory)

        // in ancient times, object were stored here.
        // but that led to many files in the same folders.
        // so we try if possible to insert new files in a deeper hierarchy
        let oldFilePath = fsPath.join(this.rootPath, prefix, sha)
        if (fs.existsSync(oldFilePath)) {
            log.dbg(`sha ${sha} is at ${oldFilePath}`)
            return oldFilePath
        }

        let prefix2 = sha.substring(0, 4)
        let directory2 = fsPath.join(this.rootPath, prefix, prefix2)
        if (!fs.existsSync(directory2))
            fs.mkdirSync(directory2)

        let filePath = fsPath.join(this.rootPath, prefix, prefix2, sha)
        log.dbg(`sha ${sha} is at ${filePath}`)
        return filePath
    }

    private async contentFileName(sha: string) {
        if (!sha)
            return null

        let prefix = sha.substring(0, 2)
        let directory = fsPath.join(this.rootPath, prefix)
        if (!await FsTools.fileExists(directory))
            await FsTools.mkdir(directory)

        // in ancient times, object were stored here.
        // but that led to many files in the same folders.
        // so we try if possible to insert new files in a deeper hierarchy
        let oldFilePath = fsPath.join(this.rootPath, prefix, sha)
        if (await FsTools.fileExists(oldFilePath)) {
            log.dbg(`sha ${sha} is at ${oldFilePath}`)
            return oldFilePath
        }

        let prefix2 = sha.substring(0, 4)
        let directory2 = fsPath.join(this.rootPath, prefix, prefix2)
        if (!await FsTools.fileExists(directory2))
            await FsTools.mkdir(directory2)

        let filePath = fsPath.join(this.rootPath, prefix, prefix2, sha)
        log.dbg(`sha ${sha} is at ${filePath}`)
        return filePath
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