import * as ClientPeering from '../ClientPeering.js'
import { HexaBackupStore } from '../HexaBackupStore.js'
import { LoggerBuilder } from '@ltearno/hexa-js'
import * as Operations from '../Operations.js'

const log = LoggerBuilder.buildLogger('misc-server')

export class Miscellanous {
    constructor(private store: HexaBackupStore) {
    }

    addEnpointsToApp(app: any) {
        app.post('/pull', async (req, res) => {
            res.set('Content-Type', 'application/json')
            
            try {
                let { sourceId, storeIp, storePort, storeToken, insecure, force } = req.body

                let remoteStore = (await ClientPeering.createClientPeeringFromWebSocket(storeIp, storePort, storeToken, null, insecure)).remoteStore

                log(`store ready`)
                log(`transferring`)

                let sourceIds = []
                if (sourceId)
                    sourceIds.push(sourceId)
                else
                    sourceIds = await remoteStore.getSources()

                for (let sourceId of sourceIds)
                    await Operations.pullSource(remoteStore, this.store, sourceId, force)

                log(`pull done`)

                res.send(JSON.stringify({
                    ok: "successfull",
                    pulledSourceIds: sourceIds
                }))
            }
            catch (err) {
                res.send(`{"error":"${err}"}`)
            }
        })
    }
}