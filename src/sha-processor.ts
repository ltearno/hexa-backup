import * as Stream from 'stream'
import * as ShaCache from './ShaCache'
import * as UploadTransferModel from './UploadTransferModel'

const log = require('./Logger')('ShaProcessor')

export class ShaProcessor extends Stream.Transform {
    constructor(private shaCache: ShaCache.ShaCache) {
        super({ objectMode: true })
    }

    _flush(callback) {
        callback()
    }

    async _transform(chunk: UploadTransferModel.FileInfo, encoding, callback: (err, data) => void) {
        let err = null
        let value = null

        if (chunk.isDirectory) {
            value = Object.assign({ contentSha: null }, chunk)
        }
        else {
            try {
                let sha = await this.shaCache.hashFile(chunk.name)
                value = Object.assign({ contentSha: sha }, chunk)
            } catch (e) {
                log(`ERROR SHAING ${e}`)
                err = e
            }
        }

        callback(err, value)
    }
}