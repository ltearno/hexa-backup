import { HexaBackupStore } from '../HexaBackupStore'
import { LoggerBuilder } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization'
import * as DbHelpers from '../DbHelpers'
import * as DbIndexation from '../DbIndexation'
import * as BackgroundJobs from '../BackgroundJobs'

const log = LoggerBuilder.buildLogger('stateful-server')

const SQL_RESULT_LIMIT = 62

export class Stateful {
    private backgroundJobs: BackgroundJobs.BackgroundJobClientApi
    private runningUpdate = false
    private runAgainWhenFinished = false

    constructor(private store: HexaBackupStore, private databaseParams: DbHelpers.DbParams, backgroundJobs: BackgroundJobs.BackgroundJobs) {
        this.backgroundJobs = backgroundJobs.createClient(`stateful-server`)

        this.store.addCommitListener((commitSha, sourceId) => {
            log(`update indices after commit ${commitSha} on source ${sourceId}`)

            if (this.runningUpdate) {
                this.runAgainWhenFinished = true
                log(`will be processed later`)
            }
            else {
                this.runningUpdate = true

                this.backgroundJobs.addJob(`update indices`, async () => {
                    try {
                        do {
                            log(`starting update indices`)
                            this.runAgainWhenFinished = false
                            await DbIndexation.updateObjectsIndex(this.store, this.databaseParams)
                            //await DbIndexation.updateMimeShaList('PHOTOS', 'image', store, this.databaseParams)
                            //await DbIndexation.updateMimeShaList('VIDEOS', 'video', store, this.databaseParams)
                            await DbIndexation.updateAudioIndex(this.store, this.databaseParams)
                            await DbIndexation.updateExifIndex(this.store, this.databaseParams)
                            log(`indices updated`)
                        }
                        while (this.runAgainWhenFinished)
                        this.runningUpdate = false
                    }
                    catch (err) {
                        this.runningUpdate = false
                        this.runAgainWhenFinished = false
                    }
                })
            }
        })
    }

    addEnpointsToApp(app: any) {
        app.get('/parents/:sha', async (req, res) => {
            res.set('Content-Type', 'application/json')

            try {
                let authorizedRefs = await Authorization.getAuthorizedRefsFromHttpRequestAsSql(req, this.store)
                if (!authorizedRefs) {
                    res.send(JSON.stringify([]))
                    return
                }

                let sha = req.params.sha

                const client = await DbHelpers.createClient(this.databaseParams)

                const query = `select distinct o.parentsha from object_parents o ${authorizedRefs !== null ?
                    `inner join object_sources os on o.parentsha=os.sha` :
                    ``} where ${authorizedRefs != null ?
                        `os.sourceId in (${authorizedRefs}) and` :
                        ''} o.sha = '${sha}' limit ${SQL_RESULT_LIMIT};`

                log.dbg(`sql:${query}`)

                let resultSet: any = await DbHelpers.dbQuery(client, query)

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
                let authorizedRefs = await Authorization.getAuthorizedRefsFromHttpRequestAsSql(req, this.store)
                if (!authorizedRefs) {
                    res.send(JSON.stringify([]))
                    return
                }

                let sha = req.params.sha

                const client = await DbHelpers.createClient(this.databaseParams)

                const query = `select distinct o.name from objects o ${authorizedRefs !== null ?
                    `inner join object_parents op on op.sha=o.sha inner join object_sources os on op.parentsha=os.sha` :
                    ``} where ${authorizedRefs != null ?
                        `os.sourceId in (${authorizedRefs}) and` :
                        ''} o.sha = '${sha}' limit ${SQL_RESULT_LIMIT};`

                log.dbg(`sql:${query}`)

                let resultSet: any = await DbHelpers.dbQuery(client, query)

                let result = resultSet.rows.map(row => row.name)

                client.end()

                res.send(JSON.stringify(result))
            }
            catch (err) {
                res.send(`{"error":"${err}"}`)
            }
        })

        app.get('/sha/:sha/info', async (req, res) => {
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

            const client = await DbHelpers.createClient(this.databaseParams)
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
                    text: `select * from objects where sha=$1`,
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
                    text: `select * from object_parents where sha=$1`,
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
                    text: `select * from object_sources where sha=$1`,
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

            client.end()

            res.send(JSON.stringify(info))
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

                let { name,
                    mimeType,
                    geoSearch,
                    dateMin,
                    dateMax,
                    offset,
                    limit,
                    noDirectory
                } = req.body

                const client = await DbHelpers.createClient(this.databaseParams)

                let selects: string[] = ['o.sha', 'o.name', 'o.mimeType', 'min(o.size) as size', 'min(o.lastWrite) as lastWrite']
                let whereConditions: string[] = []
                let groups: string[] = ['o.sha', 'o.name', 'o.mimeType']
                let joins: string[] = ['inner join object_sources os on o.sha=os.sha']

                if (mimeType && mimeType.startsWith('image/') && geoSearch) {
                    let { nw, se } = geoSearch
                    let latMin = Math.min(nw.lat, se.lat)
                    let latMax = Math.max(nw.lat, se.lat)
                    let lngMin = Math.min(nw.lng, se.lng)
                    let lngMax = Math.max(nw.lng, se.lng)

                    selects.push(`cast(oe.exif ->> 'GPSLatitude' as float) as latitude, cast(oe.exif ->> 'GPSLongitude' as float) as longitude`)
                    joins.push(`inner join object_exifs oe on o.sha=oe.sha`)
                    whereConditions.push(`cast(exif ->> 'GPSLatitude' as float)>=${latMin} and cast(exif ->> 'GPSLatitude' as float)<=${latMax} and cast(exif ->> 'GPSLongitude' as float)>=${lngMin} and cast(exif ->> 'GPSLongitude' as float)<=${lngMax}`)
                    groups.push(`cast(oe.exif ->> 'GPSLatitude' as float), cast(oe.exif ->> 'GPSLongitude' as float)`)
                }
                else if (mimeType && mimeType.startsWith('audio/')) {
                    joins.push(`left join object_audio_tags ot on o.sha=ot.sha`)
                    selects.push(`tags::json#>>'{common.title}' as title`)
                    selects.push(`tags::json#>>'{common.artist}' as artist`)
                    selects.push(`tags::json#>>'{common.album}' as album`)
                }

                if (dateMin)
                    whereConditions.push(`o.lastWrite>=${dateMin}`)

                if (dateMax)
                    whereConditions.push(`o.lastWrite<=${dateMax}`)

                whereConditions.push(`os.sourceId in (${authorizedRefs})`)

                let orderBy = ``

                if (!name)
                    name = ''
                name = name.trim()
                if (name != '') {
                    whereConditions.push(`o.name % '${name}' or o.name ilike '%${name}%'`)
                    orderBy = `order by similarity(o.name, '${name}') desc`
                }

                if (noDirectory)
                    whereConditions.push(`o.mimeType like '${mimeType}'`)
                else
                    whereConditions.push(`o.mimeType = 'application/directory' or o.isDirectory or o.mimeType like '${mimeType}'`)

                if (!offset || offset < 0)
                    offset = 1
                else
                    offset = Math.floor(offset * 1)

                if (!limit || limit < 1 || limit > SQL_RESULT_LIMIT)
                    limit = SQL_RESULT_LIMIT

                query = `select ${selects.join(', ')} from objects o ${joins.join(' ')} where ${whereConditions.map(c => `(${c})`).join(' and ')} group by ${groups.join(', ')} ${orderBy} limit ${limit} offset ${offset};`

                log.dbg(`sql:${query}`)

                let queryResult: any = await DbHelpers.dbQuery(client, query)

                const items = queryResult.rows.map(row => ({
                    sha: row.sha,
                    name: row.name,
                    mimeType: row.mimetype,
                    lastWrite: row.lastwrite * 1,
                    size: row.size * 1,
                    lat: row.latitude * 1,
                    lng: row.longitude * 1,
                    title: row.title,
                    artist: row.artist,
                    album: row.album
                }))

                client.end()

                const resultDirectories = items.filter(i => i.mimeType == 'application/directory').map(({ sha, name }) => ({ sha, name }))
                const resultFiles = items.filter(i => i.mimeType != 'application/directory')

                res.send(JSON.stringify({ resultDirectories, resultFilesddd: resultFiles, items }))
            }
            catch (err) {
                res.send(JSON.stringify({ error: err, query }))
            }
        })
    }
}