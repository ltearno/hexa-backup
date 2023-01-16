import { HexaBackupStore } from '../HexaBackupStore.js'
import { LoggerBuilder } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization.js'
import * as DbHelpers from '../DbHelpers.js'
import * as DbIndexation from '../DbIndexation.js'
import * as BackgroundJobs from '../BackgroundJobs.js'
import * as SourceState from '../SourceState.js'
import * as http from 'http'

const log = LoggerBuilder.buildLogger('stateful-server')

const SQL_RESULT_LIMIT = 62

export class Stateful {
    private backgroundJobs: BackgroundJobs.BackgroundJobClientApi
    private runningUpdate = false
    private runAgainWhenFinished = false

    constructor(private store: HexaBackupStore, private databaseParams: DbHelpers.DbParams, backgroundJobs: BackgroundJobs.BackgroundJobs) {
        this.backgroundJobs = backgroundJobs.createClient(`stateful-server`)

        this.store.addCommitListener(async (commitSha, sourceId) => {
            log(`update indices after commit ${commitSha} on source ${sourceId}`)

            let sourceState = await this.store.getSourceState(sourceId)
            if (!SourceState.isIndexed(sourceState)) {
                log(`source ${sourceId} is not indexed, skipping`)
                return
            }

            if (this.runningUpdate) {
                this.runAgainWhenFinished = true
                log(`will be processed later`)
            }
            else {
                this.runningUpdate = true

                this.backgroundJobs.addJob(`update indices`, async () => {
                    await this.updateIndices()
                })
            }
        })
    }

    private async updateIndices() {
        try {
            do {
                log(`starting update indices`)

                this.runAgainWhenFinished = false

                await DbIndexation.updateObjectsIndex(this.store, this.databaseParams)
                //await DbIndexation.updateMimeShaList('PHOTOS', 'image', store, this.databaseParams)
                //await DbIndexation.updateMimeShaList('VIDEOS', 'video', store, this.databaseParams)

                log(`indices updated`)
            }
            while (this.runAgainWhenFinished)
            this.runningUpdate = false
        }
        catch (err) {
            this.runningUpdate = false
            this.runAgainWhenFinished = false

            log.err(`exception during updateIndices: ${err}`)
        }
    }

    addEnpointsToApp(app: any) {
        app.post('/indices/update', async (req, res) => {
            res.set('Content-Type', 'application/json')
            res.send(JSON.stringify({ ok: true }))

            this.updateIndices()
        })

        app.get('/parents/:sha', async (req, res) => {
            res.set('Content-Type', 'application/json')

            try {
                let authorizedRefs = await Authorization.getAuthorizedRefsFromHttpRequestAsSql(req, this.store)
                if (!authorizedRefs) {
                    res.send(JSON.stringify([]))
                    return
                }

                let sha = req.params.sha

                const client = await DbHelpers.createClient(this.databaseParams, "shaparents")

                const query = `select distinct o.parentsha from objects_hierarchy o where ${authorizedRefs != null ?
                        `o.sourceId in (${authorizedRefs}) and` :
                        ''} o.sha = '${sha}' limit ${SQL_RESULT_LIMIT};`

                log.dbg(`sql:${query}`)

                let resultSet: any = await DbHelpers.dbQuery(client, query)

                let result = resultSet.rows.map(row => row.parentsha)

                DbHelpers.closeClient(client)

                res.send(JSON.stringify(result))
            }
            catch (err) {
                res.send(`{"error":"${err}"}`)
            }
        });

        app.get('/names/:sha', async (req, res) => {
            res.set('Content-Type', 'application/json')

            try {
                let authorizedRefs = await Authorization.getAuthorizedRefsFromHttpRequestAsSql(req, this.store)
                if (!authorizedRefs) {
                    res.send(JSON.stringify([]))
                    return
                }

                let sha = req.params.sha

                const client = await DbHelpers.createClient(this.databaseParams, "shanames")

                const query = `select distinct o.name from objects_hierarchy o where ${authorizedRefs != null ?
                        `o.sourceId in (${authorizedRefs}) and` :
                        ''} o.sha = '${sha}' limit ${SQL_RESULT_LIMIT};`

                log.dbg(`sql:${query}`)

                let resultSet: any = await DbHelpers.dbQuery(client, query)

                let result = resultSet.rows.map(row => row.name)

                DbHelpers.closeClient(client)

                res.send(JSON.stringify(result))
            }
            catch (err) {
                res.send(`{"error":"${err}"}`)
            }
        })

        app.get('/sha/:sha/stateful-info', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let sha = req.params.sha
            if (sha == null || sha == 'null') {
                res.send(JSON.stringify({ error: `input validation (sha is ${sha})` }))
                return
            }

            let authorizedRefs = await Authorization.getAuthorizedRefsFromHttpRequestAsSql(req, this.store)
            if (!authorizedRefs) {
                res.send(JSON.stringify({ error: `you are not authorized, sorry` }))
                return
            }

            const client = await DbHelpers.createClient(this.databaseParams, "shainfo")
            if (!client) {
                res.send(JSON.stringify({ error: `not connected to stateful storage (db)` }))
                return
            }

            const info = {
                sha,
                names: [],
                mimeTypes: [],
                writeDates: [],
                sizes: [],
                parents: [],
                sources: [],
                exifs: [],
                audioMetadata: [],
                errors: []
            }

            try {
                let queryResult: any = await DbHelpers.dbQuery(client, {
                    text: `select * from objects_hierarchy where sha=$1`,
                    values: [sha]
                })

                queryResult.rows.forEach(row => {
                    if (!info.names.includes(row.name))
                        info.names.push(row.name)
                    if (!info.mimeTypes.includes(row.mimetype))
                        info.mimeTypes.push(row.mimetype)
                    if (!info.writeDates.includes(row.lastwrite))
                        info.writeDates.push(row.lastwrite)
                    if (!info.sizes.includes(row.size))
                        info.sizes.push(row.size)
                })
            }
            catch (err) {
                info.errors.push(err)
            }

            try {
                let queryResult = await DbHelpers.dbQuery(client, {
                    text: `select parentsha from objects_hiearchy where sha=$1`,
                    values: [sha]
                })

                queryResult.rows.forEach(row => {
                    if (!info.parents.includes(row.parentsha))
                        info.parents.push(row.parentsha)
                })
            }
            catch (err) {
                info.errors.push(err)
            }

            try {
                let queryResult = await DbHelpers.dbQuery(client, {
                    text: `select sourceId from objects_hierarchy where sha=$1`,
                    values: [sha]
                })

                queryResult.rows.forEach(row => {
                    if (!info.sources.includes(row.sourceid))
                        info.sources.push(row.sourceid)
                })
            }
            catch (err) {
                info.errors.push(err)
            }

            try {
                let queryResult = await DbHelpers.dbQuery(client, {
                    text: `select * from object_exifs where sha=$1`,
                    values: [sha]
                })

                queryResult.rows.forEach(row => {
                    info.exifs.push(row.exif)
                })
            }
            catch (err) {
                info.errors.push(err)
            }

            try {
                let queryResult = await DbHelpers.dbQuery(client, {
                    text: `select * from object_audio_tags where sha=$1`,
                    values: [sha]
                })

                queryResult.rows.forEach(row => {
                    info.audioMetadata.push(row.tags)
                })
            }
            catch (err) {
                info.errors.push(err)
            }

            DbHelpers.closeClient(client)

            res.send(JSON.stringify(info))
        })

        function rest_get(url: string): Promise<{ statusCode: number; body: any }> {
            return new Promise((resolve, reject) => {
                const options = {
                    method: 'GET',
                }

                const req = http.request(url, options, resp => {
                    let data = ''
                    resp.on('data', chunk => data += chunk)
                    resp.on('end', () => resolve({ statusCode: resp.statusCode, body: data }))
                })

                req.on('error', err => {
                    reject(err)
                })

                req.end()
            })
        }

        app.post('/autocomplete', async (req, res) => {
            let { text } = req.body
            try {
                let { body } = await rest_get(`http://localhost:9875/complete?s=${encodeURIComponent(text)}`)
                res.set('Content-Type', 'application/json')
                res.send(JSON.stringify({ result: JSON.parse(body) }))
            } catch (e) {
                res.send(JSON.stringify({ error: "${e}", text }))
            }
        })

        app.post('/search', async (req, res) => {
            let query = '-'
            res.set('Content-Type', 'application/json')
            try {
                let authorizedRefs = await Authorization.getAuthorizedRefsFromHttpRequestAsSql(req, this.store)
                if (!authorizedRefs) {
                    res.send(JSON.stringify({ resultDirectories: [], resultFilesddd: [] }))
                    return
                }

                let {
                    name,
                    mimeType,
                    geoSearch,
                    dateMin,
                    dateMax,
                    offset,
                    limit,
                    noDirectory
                } = req.body

                const client = await DbHelpers.createClient(this.databaseParams, "search")

                let selects: string[] = ['o.sha', 'o.name', 'o.mimeType', 'min(o.size) as size', 'min(o.lastWrite) as lastWrite']
                let whereConditions: string[] = [`o.sourceId in (${authorizedRefs})`]
                let groups: string[] = ['o.sha', 'o.name', 'o.mimeType']
                let froms: string[] = ['objects_hierarchy o']
                let orders: string[] = []

                if (mimeType && mimeType.startsWith('image/') && geoSearch) {
                    let { nw, se } = geoSearch
                    let latMin = Math.min(nw.lat, se.lat)
                    let latMax = Math.max(nw.lat, se.lat)
                    let lngMin = Math.min(nw.lng, se.lng)
                    let lngMax = Math.max(nw.lng, se.lng)

                    selects.push(`cast(oe.exif ->> 'GPSLatitude' as float) as latitude, cast(oe.exif ->> 'GPSLongitude' as float) as longitude`)
                    froms.push(`inner join object_exifs oe on o.sha=oe.sha`)
                    whereConditions.push(`cast(exif ->> 'GPSLatitude' as float)>=${latMin} and cast(exif ->> 'GPSLatitude' as float)<=${latMax} and cast(exif ->> 'GPSLongitude' as float)>=${lngMin} and cast(exif ->> 'GPSLongitude' as float)<=${lngMax}`)
                    groups.push(`cast(oe.exif ->> 'GPSLatitude' as float), cast(oe.exif ->> 'GPSLongitude' as float)`)

                    if (dateMin)
                        whereConditions.push(`o.lastWrite>=${dateMin}`)
                    if (dateMax)
                        whereConditions.push(`o.lastWrite<=${dateMax}`)
                }

                if (!name)
                    name = ''
                name = name.trim()
                if (name != '') {
                    if (mimeType && mimeType.startsWith('audio/')) {
                        froms.push(`left join object_footprints of on o.sha=of.sha`)
                        whereConditions.push(`of.footprint ilike '%${name}%'`)
                        orders.push(`order by similarity(of.footprint, '${name}') desc`)
                        selects.push(`similarity(of.footprint, '${name}') as score`)
                        groups.push(`of.footprint`)
                    }
                    else {
                        whereConditions.push(`o.name % '${name}' or o.name ilike '%${name}%'`)
                        orders.push(`order by similarity(o.name, '${name}') desc`)
                        selects.push(`similarity(o.name, '${name}') as score`)
                    }
                }

                if (noDirectory)
                    whereConditions.push(`o.mimeType like '${mimeType}'`)
                else
                    whereConditions.push(`o.mimeType = 'application/x-hexa-backup-directory' or o.mimeType like '${mimeType}'`)

                if (!offset || offset < 0)
                    offset = 1
                else
                    offset = Math.floor(offset * 1)

                if (!limit || limit < 1 || limit > SQL_RESULT_LIMIT)
                    limit = SQL_RESULT_LIMIT

                query = `select ${selects.join(', ')} from ${froms.join(' ')} where ${whereConditions.map(c => `(${c})`).join(' and ')} group by ${groups.join(', ')} ${orders.join(', ')} limit ${limit} offset ${offset};`

                log(`sql:${query}`)

                let queryResult: any = await DbHelpers.dbQuery(client, query)

                const items = queryResult.rows.map(row => ({
                    sha: row.sha,
                    name: row.name,
                    mimeType: row.mimetype,
                    score: row.score * 1,

                    lastWrite: row.lastwrite * 1,
                    size: row.size * 1,

                    lat: row.latitude * 1,
                    lng: row.longitude * 1,

                    title: row.title,
                    artist: row.artist,
                    album: row.album,
                }))

                DbHelpers.closeClient(client)

                const resultDirectories = items.filter(i => i.mimeType == 'application/x-hexa-backup-directory').map(({ sha, name }) => ({ sha, name }))
                const resultFiles = items.filter(i => i.mimeType != 'application/x-hexa-backup-directory')

                res.send(JSON.stringify({ resultDirectories, resultFilesddd: resultFiles, items, query }))
            }
            catch (err) {
                res.send(JSON.stringify({ error: err, query }))
            }
        })
    }
}