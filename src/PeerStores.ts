import { LoggerBuilder } from '@ltearno/hexa-js'
import * as Operations from './Operations'
import * as ClientPeering from './ClientPeering'
import { HexaBackupStore } from './HexaBackupStore'
import * as RestTools from './RestTools'

const log = LoggerBuilder.buildLogger('peer-stores')

export interface Peer {
    connection: {
        ip: string
        port: number
        token: string
        insecure: boolean
    }

    force: boolean
    sourceIds: string[]
}

export class PeerStores {
    private peers: Peer[] = []
    private peerIndex = 0

    private delay = 1000 * 60 * 5
    private timeout: NodeJS.Timeout

    constructor(private store: HexaBackupStore) { }

    async init() {
        this.schedule()

        this.peers = await this.store.getReferenceRepository().getEx(`peers`, `peers`)
        if (!this.peers) {
            this.peers = []
        }
        else {
            log(`loaded peers : ${JSON.stringify(this.peers)}`)
        }
    }

    private schedule() {
        if (this.timeout)
            clearTimeout(this.timeout)
        this.timeout = setTimeout(() => this.scheduledTask(), this.delay)
    }

    async scheduledTask() {
        try {
            if (this.peers.length) {
                this.peerIndex = (this.peerIndex + 1) % this.peers.length

                let peer = this.peers[this.peerIndex]

                if (peer.connection.token) {
                    // try renew token
                    let response = await RestTools.post(`https://home.lteconsulting.fr/auth`, JSON.stringify({ token: peer.connection.token }), {
                        Authorization: `Bearer ${peer.connection.token}`
                    })

                    if (response.statusCode == 200) {
                        let parsedResponse = JSON.parse(response.body)
                        if (parsedResponse && parsedResponse.token) {
                            log(`new token ${parsedResponse.token}`)
                            peer.connection.token = parsedResponse.token
                        }
                    }
                }

                let ws = await Operations.connectToRemoteSocket(peer.connection.ip, peer.connection.port, peer.connection.token, peer.connection.insecure)
                if (!ws) {
                    throw (`connection impossible`)
                }

                log('connected')

                let peering = new ClientPeering.Peering(ws, false)
                peering.start().then(_ => log(`finished peering`))

                let remoteStore = peering.remoteStore
                if (!remoteStore) {
                    log.err(`cannot connect to remote store`)
                    return
                }

                log(`store ready`)
                log(`transferring`)

                let sourceIds = []
                if (peer.sourceIds)
                    sourceIds = peer.sourceIds
                else
                    sourceIds = await remoteStore.getSources()

                for (let sourceId of sourceIds)
                    await Operations.pullSource(remoteStore, this.store, sourceId, peer.force)

                log(`pull done`)
            }
        }
        catch (err) {
            log.err(`error peer-stores scheduled task (${err})`)
        }

        this.schedule()
    }

    addEnpointsToApp(app) {
        app.get('/peers', (req, res) => {
            res.set('Content-Type', 'application/json')
            res.send(JSON.stringify(this.peers))
        })

        app.post('/peers', async (req, res) => {
            res.set('Content-Type', 'application/json')
            let peer: Peer = req.body

            this.peers.push({
                connection: {
                    ip: peer.connection.ip,
                    port: peer.connection.port,
                    insecure: peer.connection.insecure,
                    token: peer.connection.token
                },
                force: !!peer.force,
                sourceIds: peer.sourceIds
            })

            res.send(JSON.stringify({ message: "ok, added peer" }))

            await this.storePeers()

            if (this.timeout)
                clearTimeout(this.timeout)
            this.scheduledTask()
        })
    }

    private async storePeers() {
        this.store.getReferenceRepository().putEx(`peers`, `peers`, this.peers)
    }
}