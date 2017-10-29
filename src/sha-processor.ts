import * as Stream from 'stream'
import * as ShaCache from './ShaCache'
import * as UploadTransferModel from './UploadTransferModel'

const log = require('./Logger')('ShaProcessor')

export class ShaProcessor extends Stream.Transform {
    constructor(private shaCache: ShaCache.ShaCache) {
        super({ objectMode: true })
    }

    async _transform(chunk: UploadTransferModel.FileInfo, encoding, callback: () => void) {
        if (chunk.isDirectory) {
            this.push(Object.assign({ contentSha: null }, chunk))
        }
        else {
            try {
                let sha = await this.shaCache.hashFile(chunk.name)
                this.push(Object.assign({ contentSha: sha }, chunk))
            }
            catch (err) {
                log(`ERROR SHAING ${err}`)
            }
        }

        callback()
    }
}