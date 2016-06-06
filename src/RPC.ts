import * as Serialization from './serialisation';


export class RPCServer {
    private engine = require('engine.io');

    listen(port: number, serviceImpl: any) {
        var server = this.engine.listen(5005);
        server.on('connection', (socket) => {
            socket.on('message', (...args) => {
                if (args == null || args.length != 1) {
                    console.log(`received corrupted message !`);
                    return;
                }

                let received = Serialization.deserialize(args[0]);

                let callId = received[0];
                received.shift();
                let method = received[0];
                received.shift();

                let m = serviceImpl[method];
                if (!m) {
                    console.log(`trying to call ${method} but it does not exist !`);
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
    private callInfos: { [key: string]: { resolver; rejecter; } } = {};
    private socket;
    private nextCallId = 1;

    constructor() {
    }

    async connect(server: string, port: number) {
        return new Promise<boolean>((resolve, reject) => {
            if (this.socket != null) {
                reject("already connected !");
                return;
            }

            this.socket = require('engine.io-client')(`ws://${server}:${port}`);
            if (!this.socket) {
                console.log('connection error');
                reject('cannot connect !');
            }
            this.socket.on('open', () => {
                console.log('received socket connection');
                this.socket.on('message', (data) => {
                    let response = Serialization.deserialize(data);

                    let callId = response[0];
                    let err = response[1];
                    let returnValue = response[2];

                    let callInfo = this.callInfos[callId];
                    delete this.callInfos[callId];

                    if (err)
                        callInfo.rejecter(err);
                    else
                        callInfo.resolver(returnValue);
                });

                this.socket.on('close', () => {
                    console.log('client connection closed');
                    this.socket = null;
                });

                this.socket.on('error', () => {
                    console.log('client connection error');
                    this.socket = null;
                    reject('error');
                });

                resolve(true);
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

                        let payload = Serialization.serialize(args);

                        that.callInfos[callId] = {
                            resolver: resolve,
                            rejecter: reject
                        };

                        that.socket.send(payload);
                    });
                };
            }
        });
    }
}