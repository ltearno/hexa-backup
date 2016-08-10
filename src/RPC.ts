import * as Serialization from './serialisation'
import * as Stream from 'stream'
import * as Net from 'net'

const log = require('./Logger')('RPC');

const RPC_MSG_CALL = 11;
const RPC_MSG_REPLY = 11;
const RPC_MSG_STREAM_START = 12;
const RPC_MSG_STREAM_STOP = 13;
const RPC_MSG_STREAM_CHUNK = 14;
const RPC_MSG_STREAM_ERROR = 15;

export class RPCServer {
    listen(port: number, serviceImpl: any) {
        let server = Net.createServer((socket) => {
            log('client connected')

            let openedStreams = {}

            socket.on('close', () => {
                log('client disconnected')

                for (let i in openedStreams)
                    openedStreams[i].receivedError("CLOSING!")
                openedStreams = {}
            })

            socketDataToMessage(socket)

            socket.on('message', (chunk) => {
                //log(`received message length: ${chunk.length}`)
                //log(`received message ${chunk.toString('hex')}`)

                let streamStub = new StreamStub(socket)
                let args = Serialization.deserialize(chunk, () => streamStub);

                let messageType: number = args[0]
                args.shift()

                //log(`received messagetype ${messageType}`)

                if (messageType == RPC_MSG_CALL) {
                    let callId = args[0];
                    let method = args[1];
                    args.shift();
                    args.shift();

                    streamStub.callId = callId
                    openedStreams[callId] = streamStub

                    let m = serviceImpl[method];
                    if (!m) {
                        log.err(`trying to call ${method} but it does not exist !`);
                        return;
                    }

                    let promise: Promise<any> = m.apply(serviceImpl, args)

                    promise.then((value) => {
                        delete openedStreams[callId]

                        let result = [RPC_MSG_REPLY, callId, null, value];
                        let resultSerialized = Serialization.serialize(result, null);
                        socketWrite(socket, resultSerialized)
                    }).catch((err) => {
                        delete openedStreams[callId]

                        let result = [RPC_MSG_REPLY, callId, err, null];
                        let resultSerialized = Serialization.serialize(result, null);
                        socketWrite(socket, resultSerialized);
                    });
                }
                else if (messageType == RPC_MSG_STREAM_CHUNK) {
                    let callId = args[0];
                    let chunk = args[1];
                    args.shift();
                    args.shift();

                    log.dbg(`received stream chunk ${chunk ? chunk.length : '(null)'}`)

                    let stub = openedStreams[callId]
                    if (!stub) {
                        log.err('no opened stream for chunk')
                        return
                    }

                    stub.received(chunk)
                }
                else if (messageType == RPC_MSG_STREAM_ERROR) {
                    let callId = args[0];
                    let error = args[1];
                    args.shift();
                    args.shift();

                    let stub = openedStreams[callId]
                    if (!stub) {
                        log.err('no opened stream for error')
                        return
                    }
                    stub.receivedError(error)
                }
                else {
                    console.error(`Received Bad Message Type : ${messageType}`)
                }
            })
        })

        server.on('error', (err) => log.err(`server error: ${err}`))

        server.listen(port)
    }
}

class StreamStub extends Stream.Readable {
    public callId: number

    constructor(private socket: Net.Socket) {
        super({ highWaterMark: 16 * 1024 * 1024 })
    }

    _read(size: number): void {
        socketWrite(this.socket, Serialization.serialize([RPC_MSG_STREAM_START, this.callId], null))
    }

    received(data) {
        if (!this.push(data))
            socketWrite(this.socket, Serialization.serialize([RPC_MSG_STREAM_STOP, this.callId], null))
    }

    receivedError(err) {
        this.emit('error', err)
    }
}

export class RPCClient {
    private callInfos: { [key: string]: { resolver; rejecter; methodName; stream: Stream.Readable; streamOpened: boolean; streamPaused: boolean; socketWriter: SocketWriter; } } = {};
    private socket: Net.Socket;
    private nextCallId = 1;

    constructor() {
    }

    async connect(server: string, port: number) {
        return new Promise<boolean>((resolve, reject) => {
            if (this.socket != null) {
                log.dbg("already connected !");
                resolve(true);
                return;
            }

            this.socket = new Net.Socket()

            this.socket.on('connect', () => {
                log(`connected to ${server}:${port}`);

                socketDataToMessage(this.socket)

                this.socket.on('message', (chunk) => {
                    //log(`received message length: ${chunk.length}`)
                    //log(`received message ${chunk.toString('hex')}`)

                    let response = Serialization.deserialize(chunk, null);

                    let messageType = response[0]

                    if (messageType == RPC_MSG_REPLY) {
                        let callId = response[1]
                        let err = response[2]
                        let returnValue = response[3]

                        log.dbg(`response of call ${callId} : ${returnValue}`);

                        let callInfo = this.callInfos[callId];
                        delete this.callInfos[callId];

                        if (err) {
                            log.err(`rpc received error processing method ${callInfo.methodName} : ${JSON.stringify(err)}`);

                            callInfo.rejecter(err);
                        }
                        else {
                            callInfo.resolver(returnValue);
                        }
                    }
                    else if (messageType == RPC_MSG_STREAM_START) {
                        let callId = response[1]
                        let callInfo = this.callInfos[callId]

                        callInfo.streamPaused = false

                        if (callInfo.streamOpened) {
                            callInfo.stream.resume()
                        }
                        else {
                            callInfo.streamOpened = true

                            log('open stream');

                            callInfo.stream.on('drain', () => {
                                //log.dbg('drain')
                                //callInfo.stream.pause()
                            })

                            callInfo.stream.on('data', (chunk) => {
                                log.dbg(`received chunk ${chunk.length}`)

                                let payload = Serialization.serialize([RPC_MSG_STREAM_CHUNK, callId, chunk], null)
                                let res = socketWrite(this.socket, payload, () => {
                                    log(`sent data through net ${payload.length}`)
                                    //if (!callInfo.streamPaused)
                                    //    callInfo.stream.resume()
                                })

                                if (!res)
                                    log('saturation RESEAU !!!!')

                                callInfo.stream.pause()
                            })

                            callInfo.stream.on('error', (error) => {
                                socketWrite(this.socket, Serialization.serialize([RPC_MSG_STREAM_ERROR, callId, error], null))
                            })

                            callInfo.stream.on('end', () => {
                                let buf = Serialization.serialize([RPC_MSG_STREAM_CHUNK, callId, null], null)
                                socketWrite(this.socket, buf)
                            })
                        }
                    }
                    else if (messageType == RPC_MSG_STREAM_STOP) {
                        let callId = response[1]
                        let callInfo = this.callInfos[callId]

                        callInfo.streamPaused = true

                        callInfo.stream.pause()
                    }
                })

                this.socket.on('close', () => {
                    log('connection closed')
                    this.socket = null
                })

                resolve(true)
            })

            this.socket.on('error', (err) => {
                log.err(`connection error ${err}`);
                this.socket = null;
                resolve(false);
            })

            this.socket.connect(port, server)
        })
    }

    createProxy<T>(): T {
        let that = this;

        return <T>new Proxy({}, {
            get(target, propKey, receiver) {
                return (...args) => {
                    return new Promise((resolve, reject) => {
                        args = args.slice();

                        args.unshift(propKey);

                        let callId = '_id_' + (that.nextCallId++);
                        args.unshift(callId);

                        args.unshift(RPC_MSG_CALL);

                        log.dbg(`call ${propKey} (${callId})`);

                        try {
                            let stream: Stream.Readable = null;
                            let payload = Serialization.serialize(args, (s) => stream = s);

                            that.callInfos[callId] = {
                                methodName: propKey,
                                resolver: resolve,
                                rejecter: reject,
                                stream: stream,
                                streamOpened: false,
                                streamPaused: false,
                                socketWriter: stream ? new SocketWriter(that.socket, callId) : null
                            };

                            log(`typeof payload: ${typeof payload}`)

                            socketWrite(that.socket, payload);
                        }
                        catch (error) {
                            log.err(`error serializing ${JSON.stringify(args)} ${error}`);
                        }
                    });
                };
            }
        });
    }
}

class SocketWriter extends Stream.Writable {
    constructor(private socket: Net.Socket, private callId) {
        super()
    }

    _write(data, encoding, callback) {
        let payload = Serialization.serialize([RPC_MSG_STREAM_CHUNK, this.callId, data], null)
        socketWrite(this.socket, payload)
    }
}

function socketWrite(socket: Net.Socket, chunk: Buffer, callback = null) {
    try {
        //log(`sending ${chunk.length} bytes`)
        //log(chunk.toString('hex'))

        let header = new Buffer(4)
        header.writeInt32LE(chunk.length, 0)
        socket.write(header)

        return socket.write(chunk, callback)
    }
    catch (e) {
        log.err(`error while writing to network: ${e}`)
        return false
    }
}

function socketDataToMessage(socket: Net.Socket) {
    let currentMessage: Buffer = null
    let currentMessageBytesToFill = 0

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
                // get length
                currentMessageBytesToFill = chunk.readInt32LE(offsetInSource)
                offsetInSource += 4

                // allocate next buffer
                currentMessage = new Buffer(currentMessageBytesToFill)
                currentMessage.fill(0xcd)

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