import { HexaBackupStore } from './HexaBackupStore.js'
import { LoggerBuilder } from '@ltearno/hexa-js'
import * as express from 'express'
import * as bodyParser from 'body-parser'
import * as http from 'http'
import * as https from 'https'
import * as Metadata from './Metadata.js'
import * as fs from 'fs'
import * as path from 'path'
import * as PeerStores from './PeerStores.js'
import * as RestApiPlugins from './rest-api/Plugins.js'
import * as RestApiRpc from './rest-api/Rpc.js'
import * as RestApiBase from './rest-api/Base.js'
import * as RestApiStateful from './rest-api/Stateful.js'
import * as Miscellanous from './rest-api/Miscellanous.js'
import * as RestApiPlaylists from './rest-api/Playlists.js'
import * as YoutubeDownload from './rest-api/YoutubeDownload.js'
import * as BackgroundJobs from './BackgroundJobs.js'

const log = LoggerBuilder.buildLogger('web-server')

type DbParams = {
    host: string
    database: string
    user: string
    password: string
    port: number
}

export async function runStore(directory: string, port: number, insecure: boolean, databaseParams: DbParams) {
    log(`preparing store and components in ${directory}`)
    let store = new HexaBackupStore(directory)

    const backgroundJobs = new BackgroundJobs.BackgroundJobs()

    const metadataServer = new Metadata.Server(directory)
    const pluginsServer = new RestApiPlugins.Plugins(store, backgroundJobs)
    const rpcServer = new RestApiRpc.Rpc(store)
    const baseServer = new RestApiBase.Base(store)
    const statefulServer = new RestApiStateful.Stateful(store, databaseParams, backgroundJobs)
    const miscServer = new Miscellanous.Miscellanous(store)
    const playlistServer = new RestApiPlaylists.Playlists(store)
    const youtubeDownloadServer = new YoutubeDownload.YoutubeDownload(store, backgroundJobs)

    const peerStores = new PeerStores.PeerStores(store)
    await peerStores.init()

    log(`web initialisation, server uuid: ${store.getUuid()}`)

    let app: any = express()

    app.use(function (req, _, next) {
        req.headers['cookie'] = ''
        log(`'${req.socket.remoteAddress}' '${req.headers['x-forwarded-for']}' ${req.method} ${req.url} '${JSON.stringify(req.headers)}'`)
        next()
    })

    app.use(bodyParser.json({ limit: '50mb', strict: false }))
    app.use(bodyParser.raw({ limit: '50mb' }))

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

    try {
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
        playlistServer.addEnpointsToApp(app)
        youtubeDownloadServer.addEnpointsToApp(app)
        backgroundJobs.addEnpointsToApp(app)

        let publicFileRoot = path.join(path.dirname(__dirname), 'static')
        log.dbg(`serving /public with ${publicFileRoot}`)
        app.use('/public', express.static(publicFileRoot))

        log(`ready on port ${port} !`)
    }
    catch (err) {
        log.err(`error in main: ${err}`)
    }
}