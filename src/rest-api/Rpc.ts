import { IHexaBackupStore } from '../HexaBackupStore'
import { Queue, Transport, NetworkApi, LoggerBuilder } from '@ltearno/hexa-js'
import {
    RequestType,
    RpcQuery,
    RpcReply
} from '../RPC'

const log = LoggerBuilder.buildLogger('rpc-server')

export class Rpc {
    constructor(private store: IHexaBackupStore) {
    }

    addEnpointsToApp(app: any) {
        app.ws('/hexa-backup', async (ws: NetworkApi.WebSocket, _req: any) => {
            log(`serving new client ws`)

            let rpcTxIn = new Queue.Queue<RpcQuery>('rpc-tx-in')
            let rpcTxOut = new Queue.Queue<{ request: RpcQuery; reply: RpcReply }>('rpc-tx-out')
            let rpcRxOut = new Queue.Queue<{ id: string; request: RpcQuery }>('rpc-rx-out')
            let rpcRxIn = new Queue.Queue<{ id: string; reply: RpcReply }>('rpc-rx-in')

            let transport = new Transport.Transport(Queue.waitPopper(rpcTxIn), Queue.directPusher(rpcTxOut), Queue.directPusher(rpcRxOut), Queue.waitPopper(rpcRxIn), ws)
            transport.start()

            ws.on('error', err => {
                log(`error on ws ${err}`)
                ws.close()
            })

            ws.on('close', () => {
                log(`closed ws`)
                rpcRxOut.push(null)
            })

            await Queue.tunnelTransform(
                Queue.waitPopper(rpcRxOut),
                Queue.directPusher(rpcRxIn),
                async (p: { id: string; request: RpcQuery }) => {
                    let { id, request } = p

                    switch (request[0]) {
                        case RequestType.HasShaBytes:
                            let count = await this.store.hasOneShaBytes(request[1])
                            return {
                                id,
                                reply: [count]
                            }

                        case RequestType.ShaBytes:
                            return {
                                id,
                                reply: [await this.store.putShaBytes(request[1], request[2], request[3])]
                            }

                        case RequestType.Call:
                            request.shift()
                            let methodName = request.shift()
                            let args = request

                            let method = this.store[methodName]
                            if (!method) {
                                log(`not found method ${methodName} in store !`)
                            }
                            try {
                                let result = await method.apply(this.store, args)

                                return {
                                    id,
                                    reply: [result]
                                }
                            }
                            catch (error) {
                                log.wrn(`error doing RPC call ${error} ${method}(${JSON.stringify(args)})`)
                                return {
                                    id,
                                    reply: [null, error]
                                }
                            }
                    }
                })

            log(`bye bye client ws !`)
        })
    }
}