import * as Stream from 'stream'
import * as ShaCache from './ShaCache'
import * as UploadTransferModel from './UploadTransferModel'
import Log from './log'

const log = Log('ShaProcessor')

export class ShaProcessor extends Stream.Transform {
    private hashedBytes = 0

    constructor(private shaCache: ShaCache.ShaCache, private stateCallback?: (hashedBytes: number) => void) {
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

                this.hashedBytes += chunk.size
                this.stateCallback && this.stateCallback(this.hashedBytes)
            }
            catch (err) {
                log(`ERROR SHAING ${err}`)
            }
        }

        callback()
    }
}