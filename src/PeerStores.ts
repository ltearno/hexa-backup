import { LoggerBuilder } from '@ltearno/hexa-js'
import * as Operations from './Operations'
import * as ClientPeering from './ClientPeering'
import { HexaBackupStore, IHexaBackupStore } from './HexaBackupStore'
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
    push: boolean
}

export class PeerStores {
    private peers: Peer[] = []
    private peerIndex = 0

    private delay = 1000 * 60 * 5
    private timeout: NodeJS.Timeout

    constructor(private store: HexaBackupStore) { }

    async init() {
        this.schedule()

        await this.loadPeers()
    }

    private async loadPeers() {
        this.peers = await this.store.getReferenceRepository().getEx(`peers`, `peers`)
        if (!this.peers) {
            this.peers = []
        }
        else {
            this.peers.forEach(peer => {
                peer.push = !!peer.push
            })

            log(`loaded peers : ${JSON.stringify(this.peers)}`)
        }
    }

    private schedule() {
        if (this.timeout)
            clearTimeout(this.timeout)
        log(`waiting ${(this.delay / 1000).toFixed(0)} seconds before next`)
        this.timeout = setTimeout(() => this.scheduledTask(), this.delay)
    }

    async scheduledTask() {
        try {
            await this.loadPeers()

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

                let remoteStore = (await ClientPeering.createClientPeeringFromWebSocket(peer.connection.ip, peer.connection.port, peer.connection.token, peer.connection.insecure, false)).remoteStore
                if (!remoteStore) {
                    log.err(`cannot connect to remote store`)
                    return
                }

                log(`store ready`)

                let sourceStore: IHexaBackupStore = remoteStore
                let destinationStore: IHexaBackupStore = this.store

                if (peer.push) {
                    let temp = sourceStore
                    sourceStore = destinationStore
                    destinationStore = temp

                    log(`transferring local to remote`)
                }
                else {
                    log(`transferring remote to local`)
                }

                let sourceIds = []
                if (peer.sourceIds)
                    sourceIds = peer.sourceIds
                else
                    sourceIds = await sourceStore.getSources()

                for (let sourceId of sourceIds)
                    await Operations.pullSource(sourceStore, destinationStore, sourceId, peer.force)

                log(`transfer done`)
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
                push: !!peer.push,
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