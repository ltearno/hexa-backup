import { HexaBackupStore } from './HexaBackupStore'
import * as Stream from 'stream'
import * as Net from 'net'
import * as UploadTransferModel from './UploadTransferModel'
import * as Model from './Model'
import { LoggerBuilder, Serialisation } from '@ltearno/hexa-js'


const log = LoggerBuilder.buildLogger('UploadTransferServer')

export class UploadTransferServer {
    listen(port: number, store: HexaBackupStore) {
        let server = Net.createServer(socket => {
            log('client connected')

            class ShaWriter extends Stream.Writable {
                constructor() {
                    super({ objectMode: true })
                }

                async _write(data, encoding, callback) {
                    if (data.offset >= 0)
                        await store.putShaBytes(data.sha, data.offset, data.buffer)
                    else
                        await store.validateShaBytes(data.sha)
                    callback(null, null)
                }
            }

            let shaWriter = new ShaWriter()
            let currentClientId = null
            let currentTxId = null

            let messageToPayloadStream = new Socket2Message.MessageToPayloadStream()
            messageToPayloadStream.pipe(socket, { end: false })

            let processStream = new Stream.Writable({ objectMode: true, highWaterMark: 30 })
            processStream._write = async (message, encoding, callback) => {
                let [messageType, param1 = null, param2 = null, param3 = null] = Serialisation.deserialize(message, null)

                switch (messageType) {
                    case UploadTransferModel.MSG_TYPE_ASK_BEGIN_TX: {
                        let clientId = param1

                        currentClientId = clientId
                        currentTxId = await store.startOrContinueSnapshotTransaction(clientId)
                        log(`begin tx ${currentTxId}`)
                        await Socket2Message.writeStreamAsync(messageToPayloadStream, Serialisation.serialize([UploadTransferModel.MSG_TYPE_REP_BEGIN_TX, currentTxId]))
                        break
                    }

                    case UploadTransferModel.MSG_TYPE_ASK_SHA_STATUS: {
                        let sha = param1
                        let size = await store.hasOneShaBytes(sha)
                        await Socket2Message.writeStreamAsync(messageToPayloadStream, Serialisation.serialize([UploadTransferModel.MSG_TYPE_REP_SHA_STATUS, [sha, size]]))
                        break
                    }

                    case UploadTransferModel.MSG_TYPE_ADD_SHA_IN_TX: {
                        let fileInfo = param1 as Model.FileDescriptor

                        await store.pushFileDescriptors(currentClientId, currentTxId, [fileInfo])
                        break
                    }

                    case UploadTransferModel.MSG_TYPE_SHA_BYTES: {
                        let sha = param1
                        let offset = param2
                        let buffer = param3

                        await Socket2Message.writeStreamAsync(shaWriter, { sha, offset, buffer })
                        break
                    }

                    case UploadTransferModel.MSG_TYPE_SHA_BYTES_COMMIT: {
                        let sha = param1

                        log(`finished sha transfer ${sha}`)

                        await Socket2Message.writeStreamAsync(shaWriter, { sha, offset: -1, buffer: null })
                        break
                    }

                    case UploadTransferModel.MSG_TYPE_COMMIT_TX: {
                        await store.commitTransaction(currentClientId, currentTxId)
                        socket.end()
                        break
                    }

                    default:
                        log.err(`unknown rx msg type ${messageType}`)
                }

                callback()
            }

            socket.on('close', () => {
                log('connection from client closed')
            })

            socket.on('error', () => {
                log('error with connection from client')
                socket.end()
            })

            socket.pipe(new Socket2Message.SocketDataToMessageStream()).pipe(processStream)
        })

        server.on('error', (err) => log.err(`server error: ${err}`))

        server.listen(port)
    }
}