import * as Net from 'net'
import * as Stream from 'stream'

const log = require('./Logger')('Socket2Message')

export function sendMessageToSocket(payload: Buffer, socket: Net.Socket) {
    return new Promise((resolve, reject) => {
        try {
            let buffer = new Buffer(4 + payload.length)
            buffer.writeInt32LE(payload.length, 0)
            payload.copy(buffer, 4, 0)
            socket.write(buffer, () => resolve())

            //let header = new Buffer(4)
            //header.writeInt32LE(payload.length, 0)
            //socket.write(header)
            //socket.write(payload, () => resolve())
        }
        catch (error) {
            reject(error)
        }
    })
}

export class MessageToPayloadStream extends Stream.Transform {
    constructor() {
        super({ objectMode: true })
    }

    _transform(payload, encoding, callback) {
        let buffer = new Buffer(4 + payload.length)
        buffer.writeInt32LE(payload.length, 0)
        payload.copy(buffer, 4, 0)
        this.push(buffer)

        /*let header = new Buffer(4)
        header.writeInt32LE(payload.length, 0)
        this.push(header)
        this.push(payload)*/

        callback()
    }
}

export class SocketDataToMessageStream extends Stream.Transform {
    private currentMessage: Buffer = null
    private currentMessageBytesToFill = 0

    private counterBuffer = new Buffer(4)
    private counterBufferOffset = 0

    constructor() {
        super({ objectMode: true })
    }

    _transform(chunk: Buffer, encoding, callback) {
        let offsetInSource = 0

        try {
            while (true) {
                if (this.currentMessageBytesToFill === 0 && this.currentMessage) {
                    this.push(this.currentMessage)
                    this.currentMessage = null
                }

                if (offsetInSource >= chunk.length)
                    break

                if (this.currentMessageBytesToFill === 0) {
                    let counterLength = 4 - this.counterBufferOffset
                    if (chunk.length - offsetInSource < counterLength)
                        counterLength = chunk.length - offsetInSource

                    chunk.copy(this.counterBuffer, this.counterBufferOffset, offsetInSource, offsetInSource + counterLength)
                    this.counterBufferOffset += counterLength
                    offsetInSource += counterLength

                    if (this.counterBufferOffset == 4) {
                        // get length
                        this.currentMessageBytesToFill = this.counterBuffer.readInt32LE(0)
                        this.counterBufferOffset = 0

                        // allocate next buffer
                        this.currentMessage = new Buffer(this.currentMessageBytesToFill)
                    }

                    continue
                }

                // copy some bytes
                let copyLength = chunk.length - offsetInSource
                if (copyLength > this.currentMessageBytesToFill)
                    copyLength = this.currentMessageBytesToFill

                if (copyLength > 0) {
                    let offsetInDest = this.currentMessage.length - this.currentMessageBytesToFill
                    chunk.copy(this.currentMessage, offsetInDest, offsetInSource, offsetInSource + copyLength)
                    this.currentMessageBytesToFill -= copyLength
                    offsetInSource += copyLength
                }
            }
        }
        catch (e) {
            log.err(`error processing socket incoming data`)
        }

        callback()
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