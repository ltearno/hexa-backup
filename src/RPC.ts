import * as Serialization from './serialisation';
import * as Stream from 'stream'

const log = require('./Logger')('RPC');

const RPC_MSG_CALL = 11;
const RPC_MSG_REPLY = 11;
const RPC_MSG_STREAM_START = 12;
const RPC_MSG_STREAM_STOP = 13;
const RPC_MSG_STREAM_CHUNK = 14;
const RPC_MSG_STREAM_ERROR = 15;

export class RPCServer {
    private engine = require('engine.io');

    private openedStreams = {}

    listen(port: number, serviceImpl: any) {
        var server = this.engine.listen(5005);
        server.on('connection', (socket) => {
            log('client connected');

            socket.on('close', () => log('client disconnected'))

            socket.on('message', (args) => {
                let streamStub = new StreamStub(socket)

                let received = Serialization.deserialize(args, () => streamStub);

                let messageType: number = received[0]
                received.shift()

                if (messageType == RPC_MSG_CALL) {
                    let callId = received[0];
                    let method = received[1];
                    received.shift();
                    received.shift();

                    streamStub.callId = callId
                    this.openedStreams[callId] = streamStub

                    let m = serviceImpl[method];
                    if (!m) {
                        log.err(`trying to call ${method} but it does not exist !`);
                        return;
                    }

                    let promise: Promise<any> = m.apply(serviceImpl, received)

                    promise.then((value) => {
                        delete this.openedStreams[callId]

                        let result = [RPC_MSG_REPLY, callId, null, value];
                        let resultSerialized = Serialization.serialize(result, null);
                        socket.send(resultSerialized)
                    }).catch((err) => {
                        delete this.openedStreams[callId]

                        let result = [RPC_MSG_REPLY, callId, err, null];
                        let resultSerialized = Serialization.serialize(result, null);
                        socket.send(resultSerialized);
                    });
                }
                else if (messageType == RPC_MSG_STREAM_CHUNK) {
                    let callId = received[0];
                    let chunk = received[1];
                    received.shift();
                    received.shift();

                    log.dbg(`received stream chunk ${chunk ? chunk.length : '(null)'}`)

                    let stub = this.openedStreams[callId]
                    if (!stub) {
                        log.err('no opened stream for chunk')
                        return
                    }
                    stub.received(chunk)
                }
                else if (messageType == RPC_MSG_STREAM_ERROR) {
                    let callId = received[0];
                    let error = received[1];
                    received.shift();
                    received.shift();

                    let stub = this.openedStreams[callId]
                    if (!stub) {
                        log.err('no opened stream for error')
                        return
                    }
                    stub.receivedError(error)
                }
                else {
                    console.error(`Received Bad Message Type : ${messageType}`)
                }
            });
        });
    }
}

class StreamStub extends Stream.Readable {
    public callId: number

    constructor(private socket) {
        super({ highWaterMark: 16 * 1024 * 1024 })
    }

    _read(size: number): void {
        this.socket.send(Serialization.serialize([RPC_MSG_STREAM_START, this.callId], null))
    }

    received(data) {
        if (!this.push(data))
            this.socket.send(Serialization.serialize([RPC_MSG_STREAM_STOP, this.callId], null))
    }

    receivedError(err) {
        this.emit('error', err)
    }
}

export class RPCClient {
    private callInfos: { [key: string]: { resolver; rejecter; methodName; stream: Stream.Readable; streamOpened: boolean; streamPaused: boolean; } } = {};
    private socket;
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

            this.socket = require('engine.io-client')(`ws://${server}:${port}`);
            if (!this.socket) {
                log.err('connection error');
                resolve(false);
            }

            this.socket.on('open', () => {
                log(`connected to ${server}:${port}`);

                this.socket.on('message', (data) => {
                    let response = Serialization.deserialize(data, null);

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

                            callInfo.stream.on('data', (chunk) => {
                                callInfo.stream.pause()
                                this.socket.send(Serialization.serialize([RPC_MSG_STREAM_CHUNK, callId, chunk], null), { compress: true }, () => {
                                    if (!callInfo.streamPaused)
                                        callInfo.stream.resume()
                                })
                            })

                            this.callInfos[callId].stream.on('error', (error) => {
                                this.socket.send(Serialization.serialize([RPC_MSG_STREAM_ERROR, callId, error], null))
                            })

                            callInfo.stream.on('end', () => {
                                let buf = Serialization.serialize([RPC_MSG_STREAM_CHUNK, callId, null], null)
                                this.socket.send(buf)
                            })
                        }
                    }
                    else if (messageType == RPC_MSG_STREAM_STOP) {
                        let callId = response[1]
                        let callInfo = this.callInfos[callId]

                        callInfo.streamPaused = true

                        this.callInfos[callId].stream.pause()
                    }
                });

                this.socket.on('close', () => {
                    log('connection closed');
                    this.socket = null;
                });

                resolve(true);
            });

            this.socket.on('error', (err) => {
                log.err(`connection error ${err}`);
                this.socket = null;
                resolve(false);
            });
        });
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
                                streamPaused: false
                            };

                            that.socket.send(payload);
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