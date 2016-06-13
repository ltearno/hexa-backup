import fs = require('fs');
import fsPath = require('path');
import * as HashTools from './HashTools';

export class ShaCache {
    private cacheDirectory: string;
    private cache: any;
    private dirtyCache: boolean = false;

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

    flushToDisk() {
        if (this.dirtyCache) {
            let cacheFileName = fsPath.join(this.cacheDirectory, 'data');
            fs.writeFileSync(cacheFileName, JSON.stringify(this.cache), 'utf8');
            this.dirtyCache = false;
        }
    }

    async hashFile(fullFileName: string): Promise<string> {
        return new Promise<string>(async (resolve, reject) => {
            if (!fsPath.isAbsolute(fullFileName))
                throw "path should be absolute";

            let stat = fs.lstatSync(fullFileName);

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

            if (stat.size > 10 * 1024 * 1024)
                this.flushToDisk();

            resolve(contentSha);
        });
    }
}