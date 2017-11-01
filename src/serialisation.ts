import * as Stream from 'stream'

class GrowingBuffer {
    private buffer: Buffer;

    constructor() {
        this.buffer = Buffer.alloc(128 * 1024);
    }

    extract(size: number) {
        let result = Buffer.alloc(size);
        this.buffer.copy(result, 0, 0, size);
        return result;
    }

    writeByte(offset: number, byte: number): number {
        this.checkEnoughBuffer(offset + 1);
        this.buffer.writeInt8(byte, offset);
        return 1;
    }

    writeUInt32(offset: number, value: number): number {
        this.checkEnoughBuffer(offset + 4);
        this.buffer.writeUInt32LE(value, offset);
        return 4;
    }

    writeBuffer(offset: number, buffer: Buffer): number {
        this.checkEnoughBuffer(offset + 4 + buffer.length);
        this.writeUInt32(offset, buffer.length);
        buffer.copy(this.buffer, offset + 4, 0, buffer.length);
        return 4 + buffer.length;
    }

    writeAny(offset: number, value: any): number {
        let payload = JSON.stringify(value);
        let buffer = Buffer.from(payload);

        return this.writeBuffer(offset, buffer);
    }

    private checkEnoughBuffer(size: number) {
        if (this.buffer.length < size) {
            let newBuffer = Buffer.alloc(size * 2);
            this.buffer.copy(newBuffer, 0, 0, this.buffer.length);
            this.buffer = newBuffer;
        }
    }
}

const TYPE_BUFFER = 0;
const TYPE_ANY = 1;
const TYPE_NULL = 2;
const TYPE_UNDEFINED = 3;
const TYPE_STREAM = 4;

// WARNING : given that the buffer is not used in a reentrant manner, this should be ok
let buffer: GrowingBuffer = new GrowingBuffer()

/**
 * WARNING This function is not reentrant !
 * @param args 
 * @param streamReceiver 
 */
export function serialize(args: any[], streamReceiver: (stream: Stream.Readable) => void = null): Buffer {
    let currentOffset = 0;
    currentOffset += buffer.writeByte(currentOffset, args.length);

    for (let p in args) {
        let arg = args[p];

        try {
            if (arg === null) {
                currentOffset += buffer.writeByte(currentOffset, TYPE_NULL)
            }
            else if (arg === undefined) {
                currentOffset += buffer.writeByte(currentOffset, TYPE_UNDEFINED)
            }
            else if (arg instanceof Stream.Stream) {
                currentOffset += buffer.writeByte(currentOffset, TYPE_STREAM)
                if (streamReceiver)
                    streamReceiver(arg as Stream.Readable)
            }
            else if (arg instanceof Buffer) {
                currentOffset += buffer.writeByte(currentOffset, TYPE_BUFFER)
                currentOffset += buffer.writeBuffer(currentOffset, arg)
            }
            else {
                currentOffset += buffer.writeByte(currentOffset, TYPE_ANY)
                currentOffset += buffer.writeAny(currentOffset, arg)
            }
        }
        catch (error) {
            throw `error serializing param ${p} ${arg} ${JSON.stringify(arg)} in ${JSON.stringify(args)} : ${error}`;
        }
    }

    let result = buffer.extract(currentOffset);

    return result;
}

export function deserialize(buffer: Buffer, streamCreator: () => Stream.Stream): any[] {
    let currentOffset = 0

    let nbItems = buffer.readUInt8(currentOffset)
    currentOffset += 1

    let result = new Array(nbItems)
    for (let i = 0; i < nbItems; i++) {
        let itemType = buffer.readUInt8(currentOffset)
        currentOffset += 1

        let param = null

        if (itemType == TYPE_BUFFER) {
            let chunkSize = buffer.readUInt32LE(currentOffset)
            currentOffset += 4

            param = buffer.slice(currentOffset, currentOffset + chunkSize)
            currentOffset += chunkSize
        }
        else if (itemType == TYPE_ANY) {
            let chunkSize = buffer.readUInt32LE(currentOffset)
            currentOffset += 4

            param = JSON.parse(buffer.slice(currentOffset, currentOffset + chunkSize).toString('utf8'))
            currentOffset += chunkSize
        }
        else if (itemType == TYPE_STREAM) {
            if (streamCreator)
                param = streamCreator()
        }
        else if (itemType == TYPE_NULL) {
            param = null;
        }
        else if (itemType == TYPE_UNDEFINED) {
            param = undefined;
        }

        result[i] = param;
    }

    return result;
}