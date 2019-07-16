import { HexaBackupStore } from './HexaBackupStore'
import { Queue, Transport, NetworkApi, LoggerBuilder } from '@ltearno/hexa-js'
import * as express from 'express'
import * as bodyParser from 'body-parser'
import * as http from 'http'
import * as https from 'https'
import {
    RequestType,
    RpcQuery,
    RpcReply
} from './RPC'
import * as Metadata from './Metadata'
import * as fs from 'fs'
import * as path from 'path'
import * as DbHelpers from './DbHelpers'
import * as Operations from './Operations'
import * as ClientPeering from './ClientPeering'
import * as VideoConverter from './VideoConverter'
import * as PeerStores from './PeerStores'
import * as Authorization from './Authorization'

const log = LoggerBuilder.buildLogger('web-server')

export async function runStore(directory: string, port: number, insecure: boolean) {
    log(`preparing store and components in ${directory}`)
    let store = new HexaBackupStore(directory)

    let metadataServer = new Metadata.Server(directory)

    let thumbnailCache = new Map<string, Buffer>()
    let thumbnailCacheEntries = []

    let mediumCache = new Map<string, Buffer>()
    let mediumCacheEntries = []

    let videoConverter = new VideoConverter.VideoConverter(store)
    videoConverter.init()

    let peerStores = new PeerStores.PeerStores(store)
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

    metadataServer.addEnpointsToApp(app)

    peerStores.addEnpointsToApp(app)

    let publicFileRoot = path.join(path.dirname(__dirname), 'static')
    log.dbg(`serving /public with ${publicFileRoot}`)
    app.use('/public', express.static(publicFileRoot))

    app.get('/parents/:sha', async (req, res) => {
        res.set('Content-Type', 'application/json')

        try {
            let user = req.headers["x-authenticated-user"] || 'anonymous'
            let tmp = await Authorization.getAuthorizedRefs(user, store)
            if (!tmp || !tmp.length) {
                res.send(JSON.stringify([]))
                return
            }

            let authorizedRefs = tmp.join(', ')

            let sha = req.params.sha

            const { Client } = require('pg')

            const client = new Client({
                user: 'postgres',
                host: 'localhost',
                database: 'postgres',
                password: 'hexa-backup',
                port: 5432,
            })
            client.connect()

            let resultSet: any = await DbHelpers.dbQuery(
                client,
                `select distinct o.parentSha from object_parents o ${authorizedRefs !== null ?
                    `inner join object_sources os on o.parentSha=os.sha` :
                    ``} where ${authorizedRefs != null ?
                        `os.sourceId in (${authorizedRefs}) and` :
                        ''} o.sha = '${sha}' limit 500;`
            )

            let result = resultSet.rows.map(row => row.parentsha)

            client.end()

            res.send(JSON.stringify(result))
        }
        catch (err) {
            res.send(`{"error":"${err}"}`)
        }
    });

    app.get('/names/:sha', async (req, res) => {
        res.set('Content-Type', 'application/json')

        try {
            let user = req.headers["x-authenticated-user"] || 'anonymous'
            let tmp = await Authorization.getAuthorizedRefs(user, store)
            if (!tmp || !tmp.length) {
                res.send(JSON.stringify([]))
                return
            }

            let authorizedRefs = tmp.join(', ')

            let sha = req.params.sha

            const { Client } = require('pg')

            const client = new Client({
                user: 'postgres',
                host: 'localhost',
                database: 'postgres',
                password: 'hexa-backup',
                port: 5432,
            })
            client.connect()

            let resultSet: any = await DbHelpers.dbQuery(client, `select distinct o.name from objects o ${authorizedRefs !== null ? `inner join object_sources os on o.parentSha=os.sha` : ``} where ${authorizedRefs != null ? `os.sourceId in (${authorizedRefs}) and` : ''} o.sha = '${sha}' limit 500;`)

            let result = resultSet.rows.map(row => row.name)

            client.end()

            res.send(JSON.stringify(result))
        }
        catch (err) {
            res.send(`{"error":"${err}"}`)
        }
    });

    app.post('/pull', async (req, res) => {
        res.set('Content-Type', 'application/json')
        try {
            let { sourceId, storeIp, storePort, storeToken, insecure, force } = req.body

            let ws = await Operations.connectToRemoteSocket(storeIp, storePort, storeToken, insecure)
            if (!ws) {
                throw (`connection impossible`)
            }

            log('connected')

            let peering = new ClientPeering.Peering(ws, false)
            peering.start().then(_ => log(`finished peering`))

            let remoteStore = peering.remoteStore

            log(`store ready`)
            log(`transferring`)

            let sourceIds = []
            if (sourceId)
                sourceIds.push(sourceId)
            else
                sourceIds = await remoteStore.getSources()

            for (let sourceId of sourceIds)
                await Operations.pullSource(remoteStore, store, sourceId, force)

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

    // todo should be moved in another program !
    app.post('/search', async (req, res) => {
        res.set('Content-Type', 'application/json')
        try {
            let user = req.headers["x-authenticated-user"] || 'anonymous'
            let tmp = await Authorization.getAuthorizedRefs(user, store)
            if (!tmp || !tmp.length) {
                res.send(JSON.stringify({ resultDirectories: [], resultFilesddd: [] }))
                return
            }

            let authorizedRefs = tmp.join(', ')

            const { Client } = require('pg')

            let { name, mimeType, geoSearch, dateMin, dateMax } = req.body

            const client = new Client({
                user: 'postgres',
                host: 'localhost',
                database: 'postgres',
                password: 'hexa-backup',
                port: 5432,
            })
            client.connect()

            let resultDirectories: any = name != '' ? await DbHelpers.dbQuery(client, `select o.sha, o.name from objects o ${authorizedRefs !== null ? `inner join object_sources os on o.sha=os.sha` : ``} where ${authorizedRefs != null ? `os.sourceId in (${authorizedRefs}) and` : ''} (o.name % '${name}' or o.name ilike '%${name}%') and o.isDirectory group by o.sha, o.name order by similarity(o.name, '${name}') desc limit 500;`) : { rows: [] }
            resultDirectories = resultDirectories.rows.map(row => ({
                sha: row.sha,
                name: row.name
            }))

            let geoSearchSelect = ''
            let geoSearchJoin = ''
            let geoSearchWhere = ''
            let geoSearchGroupBy = ''
            if (mimeType && mimeType.startsWith('image') && geoSearch) {
                let { nw, se } = geoSearch
                let latMin = Math.min(nw.lat, se.lat)
                let latMax = Math.max(nw.lat, se.lat)
                let lngMin = Math.min(nw.lng, se.lng)
                let lngMax = Math.max(nw.lng, se.lng)

                geoSearchSelect = `, cast(oe.exif ->> 'GPSLatitude' as float) as latitude, cast(oe.exif ->> 'GPSLongitude' as float) as longitude`
                geoSearchJoin = ` inner join object_exifs oe on o.sha=oe.sha`
                geoSearchWhere = ` and cast(exif ->> 'GPSLatitude' as float)>=${latMin} and cast(exif ->> 'GPSLatitude' as float)<=${latMax} and cast(exif ->> 'GPSLongitude' as float)>=${lngMin} and cast(exif ->> 'GPSLongitude' as float)<=${lngMax}`
                geoSearchGroupBy = `, cast(oe.exif ->> 'GPSLatitude' as float), cast(oe.exif ->> 'GPSLongitude' as float)`
            }

            let dateWhere = ''
            if (dateMin) {
                dateWhere = ` and o.lastWrite>=${dateMin}`
            }
            if (dateMax) {
                dateWhere += ` and o.lastWrite<=${dateMax}`
            }

            let orderBy = ``

            let nameWhere = ''
            name = name.trim()
            if (name != '') {
                nameWhere = ` and (o.name % '${name}' or o.name ilike '%${name}%')`
                orderBy = `order by similarity(o.name, '${name}') desc`
            }

            let query = `select o.sha, o.name, o.mimeType${geoSearchSelect}, min(o.size) as size, min(o.lastWrite) as lastWrite from objects o ${authorizedRefs !== null ? `inner join object_sources os on o.sha=os.sha` : ``}${geoSearchJoin} where ${authorizedRefs !== null ? `os.sourceId in (${authorizedRefs})` : '1=1'}${nameWhere} and o.mimeType != 'application/directory' and o.mimeType like '${mimeType}'${geoSearchWhere}${dateWhere} group by o.sha, o.name, o.mimeType${geoSearchGroupBy} ${orderBy} limit 500;`

            let resultFiles: any = await DbHelpers.dbQuery(client, query)
            resultFiles = resultFiles.rows.map(row => ({
                sha: row.sha,
                name: row.name,
                mimeType: row.mimetype,
                lastWrite: row.lastwrite * 1,
                size: row.size * 1,
                lat: row.latitude * 1,
                lng: row.longitude * 1
            }))

            client.end()

            res.send(JSON.stringify({ resultDirectories, resultFilesddd: resultFiles }))
        }
        catch (err) {
            res.send(`{"error":"${err}"}`)
        }
    })

    app.get('/refs', async (req, res) => {
        try {
            let user = req.headers["x-authenticated-user"] || 'anonymous'

            let refs = await Authorization.getRawAuthorizedRefs(user, store)

            res.send(JSON.stringify(refs))
        }
        catch (err) {
            res.send(`{"error":"${err}"}`)
        }
    })

    app.get('/refs/:id', async (req, res) => {
        try {
            let id = req.params.id

            let result = await store.getSourceState(id)

            res.send(JSON.stringify(result))
        }
        catch (err) {
            res.send(`{"error":"${err}"}`)
        }
    });

    app.get('/sha/:sha/content', async (req, res) => {
        let sha = req.params.sha
        if (sha == null || sha == 'null') {
            res.send(`{"error":"input validation (sha is ${sha})"}`)
            return
        }

        const range = req.headers.range

        try {
            const fileSize = await store.hasOneShaBytes(sha)

            if (range) {
                const parts = range.replace(/bytes=/, "").split("-")
                const start = parseInt(parts[0])
                const end = Math.max(start, parts[1] ? parseInt(parts[1], 10) : start == 0 ? Math.min(fileSize - 1, 100 * 1024) : fileSize - 1)
                const chunksize = (end - start) + 1
                const head = {
                    'Content-Range': `bytes ${start}-${end}/${fileSize}`,
                    'Accept-Ranges': 'bytes',
                    'Content-Length': chunksize,
                    'Content-Type': req.query.type,
                    'Cache-Control': 'private, max-age=31536000',
                    'ETag': sha
                }

                if (req.query.fileName)
                    head['Content-Disposition'] = `attachment; filename="${req.query.fileName}"`

                res.writeHead(206, head)
                store.readShaAsStream(sha, start, end).pipe(res)

                log(`range-rq ${sha} ${start}-${end}(${parts[1]})/${fileSize}`)
            }
            else {
                if (req.query.type)
                    res.set('Content-Type', req.query.type)

                if (req.query.fileName)
                    res.set('Content-Disposition', `attachment; filename="${req.query.fileName}"`)

                res.set('ETag', sha)
                res.set('Cache-Control', 'private, max-age=31536000')
                res.set('Content-Length', fileSize)

                store.readShaAsStream(sha, 0, -1).pipe(res)
            }
        }
        catch (err) {
            try {
                log.err(`error when sending byte range: ${err}`)
                res.send(`{"error":"missing sha ${sha}!"}`)
            }
            catch (err2) {
                log.err(`error when sending response: ${err2}`)
            }
        }
    });

    app.get('/sha/:sha/plugins/image/thumbnail', async (req, res) => {
        let sha = req.params.sha
        if (sha == null || sha == 'null') {
            res.send(`{"error":"input validation (sha is ${sha})"}`)
            return
        }

        try {
            if (req.query.type)
                res.set('Content-Type', req.query.type)

            let out = null
            if (thumbnailCache.has(sha)) {
                out = thumbnailCache.get(sha)
            }
            else {
                let input = await store.readShaBytes(sha, 0, -1)

                const sharp = require('sharp')

                out = await sharp(input).resize(150).toBuffer()
                thumbnailCache.set(sha, out)
                thumbnailCacheEntries.push(sha)
            }

            res.set('ETag', sha)
            res.set('Cache-Control', 'private, max-age=31536000')
            res.send(out)

            if (thumbnailCache.size > 200) {
                while (thumbnailCacheEntries.length > 50) {
                    thumbnailCache.delete(thumbnailCacheEntries.shift())
                }
            }
        }
        catch (err) {
            res.set('Content-Type', 'application/json')
            res.send(`{"error":"missing sha ${sha}!"}`)
        }
    });

    app.get('/sha/:sha/plugins/image/medium', async (req, res) => {
        let sha = req.params.sha
        if (sha == null || sha == 'null') {
            res.send(`{"error":"input validation (sha is ${sha})"}`)
            return
        }

        try {
            if (req.query.type)
                res.set('Content-Type', req.query.type)

            let out = null
            if (mediumCache.has(sha)) {
                out = mediumCache.get(sha)
            }
            else {
                let input = await store.readShaBytes(sha, 0, -1)

                const sharp = require('sharp')

                out = await sharp(input).resize(1024).toBuffer()
                mediumCache.set(sha, out)
                mediumCacheEntries.push(sha)
            }

            res.set('ETag', sha)
            res.set('Cache-Control', 'private, max-age=31536000')
            res.send(out)

            if (mediumCache.size > 20) {
                while (mediumCacheEntries.length > 5) {
                    mediumCache.delete(mediumCacheEntries.shift())
                }
            }
        }
        catch (err) {
            res.send(`{"error":"missing sha ${sha}!"}`)
        }
    })

    app.get('/sha/:sha/plugins/video/small', async (req, res) => {
        let sha = req.params.sha
        if (sha == null || sha == 'null') {
            res.send(`{"error":"input validation (sha is ${sha})"}`)
            return
        }

        try {
            res.set('Content-Type', 'video/mp4')

            let fileName = await videoConverter.createSmallVideo(sha)
            if (!fileName) {
                res.send(`{"error":"converting video content (sha is ${sha})"}`)
                return
            }

            const range = req.headers.range

            let stat = fs.statSync(fileName)
            const fileSize = stat.size

            if (range) {
                const parts = range.replace(/bytes=/, "").split("-")
                const start = parseInt(parts[0])
                const end = Math.max(start, parts[1] ? parseInt(parts[1], 10) : fileSize - 1)
                const chunksize = (end - start) + 1
                const head = {
                    'Content-Range': `bytes ${start}-${end}/${fileSize}`,
                    'Accept-Ranges': 'bytes',
                    'Content-Length': chunksize,
                    'Content-Type': 'video/mp4',
                }

                if (req.query.fileName)
                    head['Content-Disposition'] = `attachment; filename="${req.query.fileName}"`

                res.writeHead(206, head)
                fs.createReadStream(fileName, { start, end }).pipe(res)
            }
            else {
                if (req.query.type)
                    res.set('Content-Type', 'video/mp4')

                if (req.query.fileName)
                    res.set('Content-Disposition', `attachment; filename="${req.query.fileName}"`)

                res.set('Cache-Control', 'private, max-age=31536000')
                res.set('Content-Length', fileSize)

                fs.createReadStream(fileName).pipe(res)
            }
        }
        catch (err) {
            try {
                log.err(`error when sending byte range: ${err}`)
                res.send(`{"error":"missing sha ${sha}!"}`)
            }
            catch (err2) {
                log.err(`error when sending response: ${err2}`)
            }
        }
    });

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
                        let count = await store.hasOneShaBytes(request[1])
                        return {
                            id,
                            reply: [count]
                        }

                    case RequestType.ShaBytes:
                        return {
                            id,
                            reply: [await store.putShaBytes(request[1], request[2], request[3])]
                        }

                    case RequestType.Call:
                        request.shift()
                        let methodName = request.shift()
                        let args = request

                        let method = store[methodName]
                        if (!method) {
                            log(`not found method ${methodName} in store !`)
                        }
                        try {
                            let result = await method.apply(store, args)

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

    log(`ready on port ${port} !`)
}