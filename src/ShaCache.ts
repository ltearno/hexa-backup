import fs = require('fs');
import fsPath = require('path')
import * as Stream from 'stream'
import { HashTools, FsTools, LoggerBuilder } from '@ltearno/hexa-js'

const log = LoggerBuilder.buildLogger('ShaCache')

const level = require('level')

interface CacheInfo {
    lastWrite: number
    size: number
    contentSha: string
}

export class ShaCache {
    private cacheDirectory: string
    private db: any

    private totalBytesCacheHit: number = 0
    private totalHashedBytes: number = 0
    private totalTimeHashing: number = 0

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

    stats() {
        return {
            tempFilesCacheSize: Object.getOwnPropertyNames(this.temporaryFiles).length,
            totalHashedBytes: this.totalHashedBytes,
            totalTimeHashing: this.totalTimeHashing,
            totalBytesCacheHit: this.totalBytesCacheHit
        }
    }

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

        let buffer = Buffer.from(payload, 'utf8')

        await this._writeFile(this.temporaryFiles[fileId].fd, buffer, 0, buffer.byteLength, this.temporaryFiles[fileId].offset)
        this.temporaryFiles[fileId].offset += buffer.byteLength
    }

    private async _writeFile(fd: number, buffer: Buffer, offset: number, length: number, position: number) {
        return new Promise<void>(resolve => {
            fs.write(fd, buffer, offset, length, position, (err, written, buffer) => resolve())
        })
    }

    /**
     * Close the temporary file and returns a stream to read it.
     * When the stream is closed, the temp file is deleted
     */
    async closeTemporaryFileAndReadAsStream(fileId: string): Promise<Stream.Readable> {
        if (!(fileId in this.temporaryFiles))
            throw `illegal temp file id ${fileId} for close and read`

        if (!this.temporaryFiles[fileId])
            return null

        await FsTools.closeFile(this.temporaryFiles[fileId].fd)
        this.temporaryFiles[fileId] = null

        let stream = fs.createReadStream(fsPath.join(this.cacheDirectory, fileId), { encoding: 'utf8' })
        stream.on('end', () => {
            fs.unlinkSync(fsPath.join(this.cacheDirectory, fileId))
        })

        return stream
    }

    async hashFile(fullFileName: string): Promise<string> {
        if (!fsPath.isAbsolute(fullFileName))
            throw "path should be absolute"

        if (! await FsTools.fileExists(fullFileName))
            return null

        let stat = await FsTools.stat(fullFileName)
        if (!stat)
            return null

        let cacheInfo = await this.getDb(fullFileName)
        if (cacheInfo && cacheInfo.lastWrite == stat.mtime.getTime() && cacheInfo.size == stat.size) {
            this.totalBytesCacheHit += cacheInfo.size
            return cacheInfo.contentSha
        }

        let startTime = Date.now()

        cacheInfo = {
            lastWrite: stat.mtime.getTime(),
            size: stat.size,
            contentSha: await HashTools.hashFile(fullFileName)
        }

        this.totalTimeHashing += Date.now() - startTime
        this.totalHashedBytes += stat.size

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
        return new Promise<void>((resolve, reject) => {
            this.db.put(key, JSON.stringify(value), err => {
                if (err)
                    reject(err)
                else
                    resolve()
            })
        })
    }
}