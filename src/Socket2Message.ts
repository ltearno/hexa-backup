import * as Net from 'net'
import * as Stream from 'stream'

const log = require('./Logger')('Socket2Message')

export function sendMessageToSocket(payload, socket: Net.Socket) {
    let header = new Buffer(4)
    header.writeInt32LE(payload.length, 0)
    socket.write(header)
    return socket.write(payload)
}

export class MessageToPayloadStream extends Stream.Transform {
    constructor() {
        super({ objectMode: true })
    }

    _transform(payload, encoding, callback) {
        let header = new Buffer(4)
        header.writeInt32LE(payload.length, 0)
        this.push(header)

        this.push(payload)
        
        callback(null, null)
    }
}

export function socketDataToMessage(socket: Net.Socket) {
    let currentMessage: Buffer = null
    let currentMessageBytesToFill = 0

    let counterBuffer = new Buffer(4)
    let counterBufferOffset = 0

    socket.on('data', (chunk: Buffer) => {
        let offsetInSource = 0

        try {
            while (true) {
                if (currentMessageBytesToFill === 0 && currentMessage) {
                    socket.emit('message', currentMessage)
                    currentMessage = null
                }

                if (offsetInSource >= chunk.length)
                    break

                if (currentMessageBytesToFill === 0) {
                    let counterLength = 4 - counterBufferOffset
                    if (chunk.length - offsetInSource < counterLength)
                        counterLength = chunk.length - offsetInSource

                    chunk.copy(counterBuffer, counterBufferOffset, offsetInSource, offsetInSource + counterLength)
                    counterBufferOffset += counterLength
                    offsetInSource += counterLength

                    if (counterBufferOffset == 4) {
                        // get length
                        currentMessageBytesToFill = counterBuffer.readInt32LE(0)
                        counterBufferOffset = 0

                        // allocate next buffer
                        currentMessage = new Buffer(currentMessageBytesToFill)
                    }

                    continue
                }

                // copy some bytes
                let copyLength = chunk.length - offsetInSource
                if (copyLength > currentMessageBytesToFill)
                    copyLength = currentMessageBytesToFill

                if (copyLength > 0) {
                    let offsetInDest = currentMessage.length - currentMessageBytesToFill
                    chunk.copy(currentMessage, offsetInDest, offsetInSource, offsetInSource + copyLength)
                    currentMessageBytesToFill -= copyLength
                    offsetInSource += copyLength
                }
            }
        }
        catch (e) {
            log.err(`error processing socket incoming data`)
        }
    })
}



