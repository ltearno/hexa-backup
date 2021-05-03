import { IHexaBackupStore } from './HexaBackupStore'
import { LoggerBuilder, Queue, Transport, NetworkApi } from '@ltearno/hexa-js'
import {
    RequestType,
    HasShaBytes,
    RpcCall,
    RpcQuery,
    RpcReply,
    ShaBytes
} from './RPC'
import * as Operations from './Operations'

const log = LoggerBuilder.buildLogger('ClientPeering')

/**
 * This is not a good architecture (duplication with store server code, and it is not yet modular)
 * 
 * But this splits Commands.ts into two files, which is more practical...
 */

export async function createClientPeeringFromWebSocket(storeIp: string, storePort: number, storeToken: string, insecure: boolean) {
    log(`connecting to remote store ${storeIp}:${storePort} (insecure=${insecure})...`)

    let ws = await Operations.connectToRemoteSocket(storeIp, storePort, storeToken, insecure)
    if (!ws) {
        log(`connection impossible`)
        return
    }
    ws.on('close', () => log(`connection to ${storeIp}:${storePort} (insecure=${insecure}) is closed`))
    log('connected')

    let peering = new Peering(ws)
    peering.start().then(_ => log(`finished peering`))
    return peering
}

export class Peering {
    constructor(private ws: NetworkApi.WebSocket) { }

    rpcCalls = new Queue.Queue<RpcCall>('rpc-calls')

    remoteStore = this.createProxy<IHexaBackupStore>()

    private rpcTxIn = new Queue.Queue<RpcQuery>('rpc-tx-in')
    private rpcTxOut = new Queue.Queue<{ request: RpcQuery; reply: RpcReply }>('rpc-tx-out')
    private rpcRxIn = new Queue.Queue<{ id: string; reply: RpcReply }>('rpc-rx-in')
    private rpcRxOut = new Queue.Queue<{ id: string; request: RpcQuery }>('rpc-rx-out')

    private rpcResolvers = new Map<RpcQuery, (value: any) => void>()
    private rpcRejecters = new Map<RpcQuery, (value: any) => void>()

    async start() {
        let transport = new Transport.Transport(
            Queue.waitPopper(this.rpcTxIn),
            Queue.directPusher(this.rpcTxOut),
            Queue.directPusher(this.rpcRxOut),
            Queue.waitPopper(this.rpcRxIn),
            this.ws
        )
        transport.start()

        this.ws.on('close', () => {
            log(`socket closed !`)
            this.rpcRxOut.push(null)
        })

        await this.startRpcLoops()
    }

    // very experimental code
    private async startRpcLoops() {
        {
            let rpcTxPusher = Queue.waitPusher(this.rpcTxIn, 20, 10)

            let rpcQueues: any[] = [{ queue: this.rpcCalls, listener: null }]

            Queue.manyToOneTransfert(rpcQueues, rpcTxPusher).then(_ => rpcTxPusher(null))
        }

        let popper = Queue.waitPopper(this.rpcTxOut)

        while (true) {
            let rpcItem = await popper()
            if (!rpcItem)
                break

            let { request, reply } = rpcItem
            switch (request[0]) {
                case RequestType.Call:
                    if (reply.length == 1) {
                        log.dbg(`rcv reply ${JSON.stringify(reply)} for request ${JSON.stringify(request)}`)
                        this.rpcResolvers.get(request)(reply[0])
                    }
                    else if (reply.length > 1) {
                        log(`exception received as a result ${JSON.stringify(reply[1])} call ${JSON.stringify(request)}`)
                        this.rpcRejecters.get(request)(reply[1])
                    }
                    this.rpcResolvers.delete(request)
                    this.rpcRejecters.delete(request)
                    break

                default:
                    log.err(`bad message received`)
                    break
            }
        }

        log(`finished rpc replies queue`)
    }

    private async callRpcOn(rpcCall: RpcCall, queue: Queue.Queue<RpcQuery>): Promise<any> {
        let result = new Promise((resolve, reject) => {
            this.rpcResolvers.set(rpcCall, resolve)
            this.rpcRejecters.set(rpcCall, reject)
        })

        await Queue.waitAndPush(queue, rpcCall, 10, 8)

        return result
    }

    private createProxy<T>(): T {
        let me = this
        return <T>new Proxy({}, {
            get(_, propKey) {
                return (...args) => {
                    args = args.slice()
                    args.unshift(propKey)
                    args.unshift(RequestType.Call)

                    return me.callRpcOn(args as RpcCall, me.rpcCalls)
                }
            }
        })
    }
}