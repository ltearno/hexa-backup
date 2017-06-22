import { HexaBackupStore } from './HexaBackupStore'
import * as Stream from 'stream'
import * as Net from 'net'
import * as UploadTransferModel from './UploadTransferModel'
import * as Serialization from './serialisation'
import * as Socket2Message from './Socket2Message'
import * as Model from './Model'

const log = require('./Logger')('UploadTransferServer')

export class UploadTransferServer {
    listen(port: number, store: HexaBackupStore) {
        let server = Net.createServer((socket) => {
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
                    callback()
                }
            }

            let shaWriter = new ShaWriter()
            let currentClientId = null
            let currentTxId = null

            socket.on('message', async (message) => {
                let [messageType, param1 = null, param2 = null, param3 = null] = Serialization.deserialize(message, null)

                switch (messageType) {
                    case UploadTransferModel.MSG_TYPE_ASK_BEGIN_TX: {
                        let clientId = param1

                        currentClientId = clientId
                        currentTxId = await store.startOrContinueSnapshotTransaction(clientId)
                        log(`begin tx ${currentTxId}`)
                        Socket2Message.sendMessageToSocket(Serialization.serialize([UploadTransferModel.MSG_TYPE_REP_BEGIN_TX, currentTxId]), socket)
                        break
                    }

                    case UploadTransferModel.MSG_TYPE_ASK_SHA_STATUS: {
                        let sha = param1
                        let reqId = param2
                        let size = await store.hasOneShaBytes(sha)
                        Socket2Message.sendMessageToSocket(Serialization.serialize([UploadTransferModel.MSG_TYPE_REP_SHA_STATUS, [reqId, size]]), socket)
                        break
                    }

                    case UploadTransferModel.MSG_TYPE_ADD_SHA_IN_TX: {
                        let fileInfo = param1 as Model.FileDescriptor
                        log(`added ${fileInfo.name}`)

                        store.pushFileDescriptors(currentClientId, currentTxId, [fileInfo])
                        break
                    }

                    case UploadTransferModel.MSG_TYPE_SHA_BYTES: {
                        let sha = param1
                        let offset = param2
                        let buffer = param3

                        shaWriter.write({ sha, offset, buffer })
                        break
                    }

                    case UploadTransferModel.MSG_TYPE_SHA_BYTES_COMMIT: {
                        let sha = param1

                        log(`finished sha transfer ${sha}`)

                        shaWriter.write({ sha, offset: -1, buffer: null })
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
            })

            Socket2Message.socketDataToMessage(socket)

            socket.on('close', () => {
                log('connection from client closed')
            })
        })

        server.on('error', (err) => log.err(`server error: ${err}`))

        server.listen(port)
    }
}