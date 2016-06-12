import * as Serialization from './serialisation';
const log = require('./Logger')('RPC');

export class RPCServer {
    private engine = require('engine.io');

    listen(port: number, serviceImpl: any) {
        var server = this.engine.listen(5005);
        server.on('connection', (socket) => {
            log('client connected');

            socket.on('close', () => log('client disconnected'))

            socket.on('message', (...args) => {
                if (args == null || args.length != 1) {
                    log.err(`received corrupted message !`);
                    return;
                }

                let received = Serialization.deserialize(args[0]);

                //log.dbg(`received ${JSON.stringify(received)}`);

                let callId = received[0];
                received.shift();
                let method = received[0];
                received.shift();

                let m = serviceImpl[method];
                if (!m) {
                    log.err(`trying to call ${method} but it does not exist !`);
                    return;
                }

                let promise: Promise<any> = m.apply(serviceImpl, received);
                promise.then((value) => {
                    let result = [callId, null, value];
                    let resultSerialized = Serialization.serialize(result);
                    socket.send(resultSerialized);
                }).catch((err) => {
                    let result = [callId, err, null];
                    let resultSerialized = Serialization.serialize(result);
                    socket.send(resultSerialized);
                });
            });
        });
    }
}

export class RPCClient {
    private callInfos: { [key: string]: { resolver; rejecter; methodName; } } = {};
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
                log('connected to server');
                this.socket.on('message', (data) => {
                    let response = Serialization.deserialize(data);

                    let callId = response[0];
                    let err = response[1];
                    let returnValue = response[2];

                    log.dbg(`response of call ${callId} : ${returnValue}`);

                    let callInfo = this.callInfos[callId];
                    delete this.callInfos[callId];

                    if (err) {
                        log.err(`rpc received error processing method ${callInfo.methodName} : ${err + ''}`);

                        callInfo.rejecter(err);
                    }
                    else
                        callInfo.resolver(returnValue);
                });

                this.socket.on('close', () => {
                    log('client connection closed');
                    this.socket = null;
                });

                resolve(true);
            });

            this.socket.on('error', () => {
                log.err('client connection error');
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

                        log.dbg(`call ${propKey} (${callId})`);

                        try {
                            let payload = Serialization.serialize(args);
                            that.callInfos[callId] = {
                                methodName: propKey,
                                resolver: resolve,
                                rejecter: reject
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