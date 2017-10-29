import * as FsTools from './FsTools';
import fs = require('fs');
import fsPath = require('path');
import * as HashTools from './HashTools';
import * as Stream from 'stream'

const log = require('./Logger')('ShaCache');

export class ShaCache {
    private cacheDirectory: string;
    private cache: any;
    private dirtyCache: boolean = false;
    private flushInterval = null

    constructor(cacheDirectory: string) {
        this.cacheDirectory = fsPath.resolve(cacheDirectory);
        if (!fs.existsSync(this.cacheDirectory))
            fs.mkdirSync(this.cacheDirectory);

        try {
            let cacheFileName = fsPath.join(this.cacheDirectory, 'data');
            if (fs.existsSync(cacheFileName))
                this.cache = JSON.parse(fs.readFileSync(cacheFileName, 'utf8'));
            else
                this.cache = {};
        }
        catch (error) {
            this.cache = {};
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

    appendToTemporaryFile(fileId: string, payload: string) {
        if (!(fileId in this.temporaryFiles))
            throw `illegal temp file id ${fileId}`

        if (!this.temporaryFiles[fileId]) {
            this.temporaryFiles[fileId] = {
                fd: fs.openSync(fsPath.join(this.cacheDirectory, fileId), 'wx'),
                offset: 0
            }

            if (!this.temporaryFiles[fileId].fd)
                throw `cannot open temp file ${fileId}`
        }

        let buffer = new Buffer(payload, 'utf8')

        fs.writeSync(this.temporaryFiles[fileId].fd, buffer, 0, buffer.byteLength, this.temporaryFiles[fileId].offset)
        this.temporaryFiles[fileId].offset += buffer.byteLength
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

        fs.closeSync(this.temporaryFiles[fileId].fd)
        delete this.temporaryFiles[fileId]

        let stream = fs.createReadStream(fsPath.join(this.cacheDirectory, fileId), { encoding: 'utf8' })

        stream.on('end', () => {
            fs.unlinkSync(fsPath.join(this.cacheDirectory, fileId))
        })

        return stream
    }

    persist() {
        this.flushToDisk()
    }

    private flushToDisk() {
        if (this.dirtyCache) {
            log.dbg(`STORING SHA CACHE...`)
            let cacheFileName = fsPath.join(this.cacheDirectory, 'data');
            fs.writeFileSync(cacheFileName, JSON.stringify(this.cache), { encoding: 'utf8' });
            this.dirtyCache = false;
            log.dbg(`STORED SHA CACHE`)
        }
    }

    async hashFile(fullFileName: string): Promise<string> {
        return new Promise<string>(async (resolve, reject) => {
            if (!fsPath.isAbsolute(fullFileName))
                reject("path should be absolute")

            let stat = await FsTools.stat(fullFileName);

            if (fullFileName in this.cache) {
                let cacheInfo = this.cache[fullFileName];
                if (cacheInfo.lastWrite == stat.mtime.getTime() && cacheInfo.size == stat.size) {
                    resolve(cacheInfo.contentSha);
                    return;
                }
            }

            let contentSha = await HashTools.hashFile(fullFileName);
            let cacheInfo = {
                lastWrite: stat.mtime.getTime(),
                size: stat.size,
                contentSha: contentSha
            };

            this.cache[fullFileName] = cacheInfo;
            this.dirtyCache = true;

            if (!this.flushInterval) {
                this.flushInterval = setInterval(() => {
                    this.flushToDisk()
                    this.flushInterval = null
                }, 30000)
            }

            resolve(contentSha);
        });
    }
}