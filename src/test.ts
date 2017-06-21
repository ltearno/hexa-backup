import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as FS from 'fs'
import * as Stream from 'stream'
import * as Net from 'net'
import * as Serialization from './serialisation'
import { ShaCache } from './ShaCache'

const log = require('./Logger')('Tests');


interface FileInfo {
    name: string;
    isDirectory: boolean;
    lastWrite: number;
    size: number;
}

interface FileAndShaInfo extends FileInfo {
    contentSha: string;
}

class ShaProcessor extends Stream.Transform {
    private shaCache: ShaCache

    constructor() {
        super({ objectMode: true })

        this.shaCache = new ShaCache('d:\\tmp\\tmp\\exp-cache')
    }

    async _transform(chunk: FileInfo, encoding, callback: (err, data) => void) {
        if (chunk.isDirectory) {
            callback(null, Object.assign({ contentSha: null }, chunk))
        }
        else {
            try {
                let sha = await this.shaCache.hashFile(chunk.name)

                let o: FileAndShaInfo = Object.assign({ contentSha: sha }, chunk)
                callback(null, o)
            } catch (err) {
                log(`ERROR SHAING ${err}`)
            }
        }
    }
}

class DirectoryLister extends Stream.Readable {
    private stack: string[]
    private awaitingReaddir: boolean = false

    constructor(private path: string, private ignoredNames: string[]) {
        super({ objectMode: true })

        this.stack = [this.path]
    }

    async _read(size: number) {
        if (this.awaitingReaddir)
            return

        if (this.stack.length > 0) {
            let currentPath = this.stack.pop();

            this.awaitingReaddir = true
            let files = await FsTools.readDir(currentPath);
            this.awaitingReaddir = false

            for (let key in files) {
                let fileName = files[key];
                if (this.ignoredNames.some(name => fileName == name))
                    continue;

                let fullFileName = fsPath.join(currentPath, fileName);
                let stat = fs.statSync(fullFileName);

                let desc: FileInfo = {
                    name: fullFileName,
                    isDirectory: stat.isDirectory(),
                    lastWrite: stat.mtime.getTime(),
                    size: 0
                };

                if (stat.isDirectory())
                    this.stack.push(fullFileName)
                else
                    desc.size = stat.size

                this.push(desc)
            }
        }

        if (!this.awaitingReaddir && this.stack.length == 0) {
            log(`END OF LIST OF FILES`)
            this.push(null)
        }
    }
}





let port = 5001

let server = Net.createServer((socket) => {
    log('client connected')

    socket.on('message', (message) => {
        let response = Serialization.deserialize(message, null)

        //log(`received message of length ${message.length} from client`)
    })

    socketDataToMessage(socket)

    socket.on('close', () => {
        log('connection from client closed')
    })
})

server.on('error', (err) => log.err(`server error: ${err}`))

server.listen(port)




let socket = new Net.Socket()

socket.on('connect', () => {
    log(`connected to ${server}:${port}`)

    socket.on('message', (message) => {
        //let response = Serialization.deserialize(chunk, null)
        //log(`received message of length ${chunk.length} from server`)
    })

    socketDataToMessage(socket)

    socket.on('close', () => {
        log('connection to server closed')
    })
})

socket.connect(port, "localhost")







let directoryLister = new DirectoryLister('d:\\tmp\\tmp', ['.git', 'exp-cache'])
let shaProcessor = new ShaProcessor()


//let readable = FS.createReadStream('D:\\Tmp\\tmp\\MVI_0545.MOV')

let piped = directoryLister.pipe(shaProcessor)

piped.on('data', (chunk: Buffer) => {
    //log(`send data to network`)

    log(`piped ${JSON.stringify(chunk)}`)

    let payload = Serialization.serialize([chunk], (s) => s)

    let header = new Buffer(4)
    header.writeInt32LE(payload.length, 0)
    socket.write(header)
    let isDraining = socket.write(payload)
    if (!isDraining) {
        //log(`network full`)
        piped.pause()
    }
})

socket.on('drain', () => {
    //log(`network drain`)
    piped.resume()
})

piped.on('end', () => {
    log(`finished reading file`)
    socket.end()
})



function socketDataToMessage(socket: Net.Socket) {
    let currentMessage: Buffer = null
    let currentMessageBytesToFill = 0

    let counterBuffer = new Buffer(4)
    let counterBufferOffset = 0

    socket.on('data', (chunk: Buffer) => {
        let offsetInSource = 0

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
    })
}



