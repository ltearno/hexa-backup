import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as FS from 'fs'
import * as Stream from 'stream'
import * as Net from 'net'
import * as Serialization from './serialisation'
import { ShaCache } from './ShaCache'
import { HexaBackupStore } from './HexaBackupStore'

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


const MSG_TYPE_ASK_SHA_STATUS = 0
const MSG_TYPE_REP_SHA_STATUS = 1
const MSG_TYPE_ADD_SHA_IN_TX = 2


let port = 5001

let server = Net.createServer((socket) => {
    log('client connected')

    let store = new HexaBackupStore('D:\\tmp\\tmp-store')

    socket.on('message', async (message) => {
        let [messageType, content] = Serialization.deserialize(message, null)

        switch (messageType) {
            case MSG_TYPE_ASK_SHA_STATUS:
                let sha = content
                let size = await store.hasOneShaBytes(sha)
                sendPayloadToSocket(Serialization.serialize([MSG_TYPE_REP_SHA_STATUS, [sha, size]]), socket)
                break

            case MSG_TYPE_ADD_SHA_IN_TX:
                let fileInfo = content as FileAndShaInfo
                log(`RCV ADD SHA IN TX ${JSON.stringify(fileInfo)}`)
                break

            default:
                log.err(`unknown rx msg type ${messageType}`)
        }
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

    initCommunication(socket)
})

socket.connect(port, "localhost")



function sendPayloadToSocket(payload, socket) {
    let header = new Buffer(4)
    header.writeInt32LE(payload.length, 0)
    socket.write(header)
    return socket.write(payload)
}


function initCommunication(socket: Net.Socket) {
    let readyAskShaStatusPayloads = []

    let readyAddFileInTxPayloads = []

    let pendingAskShaStatus: FileAndShaInfo[] = []

    let pendingSendShaBytes: { fileInfo: FileAndShaInfo; offset: number; }[] = []

    function writeSomeData() {
        let isDraining = true

        while (isDraining && readyAddFileInTxPayloads.length > 0) {
            let payload = readyAddFileInTxPayloads.shift()

            isDraining = sendPayloadToSocket(payload, socket)
        }

        while (isDraining && false) {
        }

        while (isDraining && readyAskShaStatusPayloads.length > 0) {
            let payload = readyAskShaStatusPayloads.shift()

            isDraining = sendPayloadToSocket(payload, socket)
            if (!isDraining && filesAndShasPipe)
                filesAndShasPipe.pause()
        }

        if (isDraining && filesAndShasPipe)
            filesAndShasPipe.resume()
    }

    socket.on('message', (message) => {
        let [messageType, content] = Serialization.deserialize(message, null)

        switch (messageType) {
            case MSG_TYPE_REP_SHA_STATUS:
                let sha = content[0]
                let size = content[1]

                let matchedPending = pendingAskShaStatus.shift()

                log(`received size for ${matchedPending.contentSha == sha} sha ${sha} : ${size}`)

                if (matchedPending.size != size) {
                    if (size < matchedPending.size)
                        pendingSendShaBytes.push({ fileInfo: matchedPending, offset: size })
                    else {
                        log(`warning : remote sha ${sha} is bigger than expected, restarting transfer`)
                        pendingSendShaBytes.push({ fileInfo: matchedPending, offset: 0 })
                    }
                }
                else {
                    readyAddFileInTxPayloads.push(Serialization.serialize([MSG_TYPE_ADD_SHA_IN_TX, matchedPending]))
                }

                writeSomeData()

                break

            default:
                log.err(`received unknown msg type ${messageType}`)
        }
    })

    let directoryLister = new DirectoryLister('d:\\tmp\\tmp', ['.git', 'exp-cache'])
    let shaProcessor = new ShaProcessor()
    let filesAndShasPipe = directoryLister.pipe(shaProcessor)
    filesAndShasPipe.on('data', (fileAndShaInfo: FileAndShaInfo) => {
        if (!fileAndShaInfo.isDirectory) {
            readyAddFileInTxPayloads.push(Serialization.serialize([MSG_TYPE_ADD_SHA_IN_TX, fileAndShaInfo]))
        }
        else {
            readyAskShaStatusPayloads.push(Serialization.serialize([MSG_TYPE_ASK_SHA_STATUS, fileAndShaInfo.contentSha]))
            pendingAskShaStatus.push(fileAndShaInfo)
        }

        writeSomeData()
    })

    socket.on('drain', () => {
        writeSomeData()
    })

    filesAndShasPipe.on('end', () => {
        log(`finished reading file list and hashing`)
        filesAndShasPipe = null
    })

    socketDataToMessage(socket)

    socket.on('close', () => {
        log('connection to server closed')
    })
}




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



