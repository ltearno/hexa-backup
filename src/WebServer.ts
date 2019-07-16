import { HexaBackupStore } from './HexaBackupStore'
import { LoggerBuilder } from '@ltearno/hexa-js'
import * as express from 'express'
import * as bodyParser from 'body-parser'
import * as http from 'http'
import * as https from 'https'
import * as Metadata from './Metadata'
import * as fs from 'fs'
import * as path from 'path'
import * as PeerStores from './PeerStores'
import * as RestApiPlugins from './rest-api/Plugins'
import * as RestApiRpc from './rest-api/Rpc'
import * as RestApiBase from './rest-api/Base'
import * as RestApiStateful from './rest-api/Stateful'
import * as Miscellanous from './rest-api/Miscellanous'

const log = LoggerBuilder.buildLogger('web-server')

export async function runStore(directory: string, port: number, insecure: boolean) {
    log(`preparing store and components in ${directory}`)
    let store = new HexaBackupStore(directory)

    const metadataServer = new Metadata.Server(directory)
    const pluginsServer = new RestApiPlugins.Plugins(store)
    const rpcServer = new RestApiRpc.Rpc(store)
    const baseServer = new RestApiBase.Base(store)
    const statefulServer = new RestApiStateful.Stateful(store)
    const miscServer = new Miscellanous.Miscellanous(store)
    const peerStores = new PeerStores.PeerStores(store)
    await peerStores.init()

    log(`web initialisation, server uuid: ${store.getUuid()}`)

    let app: any = express()

    app.use(bodyParser.json())

    app.use((_req, res, next) => {
        res.header("Access-Control-Allow-Origin", "*")
        res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
        next()
    })

    let server: any = null

    if (insecure) {
        server = http.createServer(app)
    }
    else {
        const CERT_KEY = 'server.key'
        const CERT_PUB = 'server.crt'
        if (!fs.existsSync(CERT_KEY) || !fs.existsSync(CERT_PUB)) {
            console.error(`error, no certificates found. Generate your certificates or use the --insecure option !\n\nyou can generate self-signed certificates with this command:\nopenssl req -new -x509 -sha256 -newkey rsa:4096 -nodes -keyout server.key -days 365 -out server.crt`)
            return
        }

        let key = fs.readFileSync(CERT_KEY)
        let cert = fs.readFileSync(CERT_PUB)
        server = https.createServer({ key, cert }, app)
    }

    server.listen(port)
    log(`listening ${insecure ? 'HTTP' : 'HTTPS'} on ${port}`)

    require('express-ws')(app, server)

    pluginsServer.addEnpointsToApp(app)
    metadataServer.addEnpointsToApp(app)
    peerStores.addEnpointsToApp(app)
    rpcServer.addEnpointsToApp(app)
    baseServer.addEnpointsToApp(app)
    statefulServer.addEnpointsToApp(app)
    miscServer.addEnpointsToApp(app)

    let publicFileRoot = path.join(path.dirname(__dirname), 'static')
    log.dbg(`serving /public with ${publicFileRoot}`)
    app.use('/public', express.static(publicFileRoot))

    log(`ready on port ${port} !`)
}