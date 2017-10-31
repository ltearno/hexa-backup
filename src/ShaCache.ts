import * as FsTools from './FsTools';
import fs = require('fs');
import fsPath = require('path');
import * as HashTools from './HashTools';
import * as Stream from 'stream'

const log = require('./Logger')('ShaCache')

const level = require('level')

interface CacheInfo {
    lastWrite: number
    size: number
    contentSha: string
}

export class ShaCache {
    private cacheDirectory: string
    private db: any

    constructor(cacheDirectory: string) {
        this.cacheDirectory = fsPath.resolve(cacheDirectory)
        if (!fs.existsSync(this.cacheDirectory))
            fs.mkdirSync(this.cacheDirectory)
        try {
            let cacheFileName = fsPath.join(this.cacheDirectory, 'data.level.db')
            this.db = level(cacheFileName)
        }
        catch (error) {
            this.db = null
        }
    }

    private temporaryFiles: { [key: string]: { fd: number; offset: number; } } = {}

    /**
     * Returns the id of the temporary file
     */
    createTemporaryFile(): string {
        let id = `temp_${Date.now()}`
        this.temporaryFiles[id] = null
        return id
    }

    async appendToTemporaryFile(fileId: string, payload: string) {
        if (!(fileId in this.temporaryFiles))
            throw `illegal temp file id ${fileId}`

        if (!this.temporaryFiles[fileId]) {
            this.temporaryFiles[fileId] = {
                fd: await FsTools.openFile(fsPath.join(this.cacheDirectory, fileId), 'wx'),
                offset: 0
            }

            if (!this.temporaryFiles[fileId].fd)
                throw `cannot open temp file ${fileId}`
        }

        let buffer = new Buffer(payload, 'utf8')

        await this._writeFile(this.temporaryFiles[fileId].fd, buffer, 0, buffer.byteLength, this.temporaryFiles[fileId].offset)
        this.temporaryFiles[fileId].offset += buffer.byteLength
    }

    private async _writeFile(fd: number, buffer: Buffer, offset: number, length: number, position: number) {
        return new Promise(resolve => {
            fs.write(fd, buffer, offset, length, position, (err, written, buffer) => resolve())
        })
    }

    /**
     * Close the temporary file and returns a stream to read it.
     * When the stream is closed, the temp file is deleted
     */
    closeTemporaryFileAndReadAsStream(fileId: string): Stream.Readable {
        if (!(fileId in this.temporaryFiles))
            throw `illegal temp file id ${fileId} for close and read`

        if (!this.temporaryFiles[fileId])
            return null

        fs.close(this.temporaryFiles[fileId].fd)
        delete this.temporaryFiles[fileId]

        let stream = fs.createReadStream(fsPath.join(this.cacheDirectory, fileId), { encoding: 'utf8' })

        stream.on('end', () => {
            fs.unlink(fsPath.join(this.cacheDirectory, fileId))
        })

        return stream
    }

    async hashFile(fullFileName: string): Promise<string> {
        if (!fsPath.isAbsolute(fullFileName))
            throw "path should be absolute"

        let stat = await FsTools.stat(fullFileName)

        let cacheInfo = await this.getDb(fullFileName)
        if (cacheInfo && cacheInfo.lastWrite == stat.mtime.getTime() && cacheInfo.size == stat.size)
            return cacheInfo.contentSha

        cacheInfo = {
            lastWrite: stat.mtime.getTime(),
            size: stat.size,
            contentSha: await HashTools.hashFile(fullFileName)
        }

        await this.putDb(fullFileName, cacheInfo)

        return cacheInfo.contentSha
    }

    private getDb(key: string): Promise<CacheInfo> {
        return new Promise((resolve, reject) => {
            this.db.get(key, (err, value) => {
                if (err)
                    resolve(null)
                else
                    resolve(value && JSON.parse(value))
            })
        })
    }

    private putDb(key: string, value: CacheInfo) {
        return new Promise((resolve, reject) => {
            this.db.put(key, JSON.stringify(value), err => {
                if (err)
                    reject(err)
                else
                    resolve()
            })
        })
    }
}