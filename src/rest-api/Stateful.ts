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
                            //await DbIndexation.updateExifIndex(this.store, this.databaseParams)
                            log(`indices updated`)
                        }
                        while (this.runAgainWhenFinished)
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
        });

        // todo should be moved in another program !
        app.post('/search', async (req, res) => {
            res.set('Content-Type', 'application/json')
            try {
                let authorizedRefs = await Authorization.getAuthorizedRefsFromHttpRequestAsSql(req, this.store)
                if (!authorizedRefs) {
                    res.send(JSON.stringify({ resultDirectories: [], resultFilesddd: [] }))
                    return
                }

                let { name, mimeType, geoSearch, dateMin, dateMax, limit } = req.body

                const client = await DbHelpers.createClient(this.databaseParams)

                let whereConditions: string[] = []

                let geoSearchSelect = ''
                let geoSearchJoin = ''
                let geoSearchGroupBy = ''
                if (mimeType && mimeType.startsWith('image') && geoSearch) {
                    let { nw, se } = geoSearch
                    let latMin = Math.min(nw.lat, se.lat)
                    let latMax = Math.max(nw.lat, se.lat)
                    let lngMin = Math.min(nw.lng, se.lng)
                    let lngMax = Math.max(nw.lng, se.lng)

                    geoSearchSelect = `, cast(oe.exif ->> 'GPSLatitude' as float) as latitude, cast(oe.exif ->> 'GPSLongitude' as float) as longitude`
                    geoSearchJoin = ` inner join object_exifs oe on o.sha=oe.sha`
                    whereConditions.push(`cast(exif ->> 'GPSLatitude' as float)>=${latMin} and cast(exif ->> 'GPSLatitude' as float)<=${latMax} and cast(exif ->> 'GPSLongitude' as float)>=${lngMin} and cast(exif ->> 'GPSLongitude' as float)<=${lngMax}`)
                    geoSearchGroupBy = `, cast(oe.exif ->> 'GPSLatitude' as float), cast(oe.exif ->> 'GPSLongitude' as float)`
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

                whereConditions.push(`o.mimeType = 'application/directory' or o.isDirectory or o.mimeType like '${mimeType}'`)

                if (!limit || limit > SQL_RESULT_LIMIT)
                    limit = SQL_RESULT_LIMIT

                const query = `select o.sha, o.name, o.mimeType ${geoSearchSelect}, min(o.size) as size, min(o.lastWrite) as lastWrite 
                    from objects o inner join object_sources os on o.sha=os.sha${geoSearchJoin} 
                    where ${whereConditions.map(c => `(${c})`).join(' and ')} 
                    group by o.sha, o.name, o.mimeType${geoSearchGroupBy} 
                    ${orderBy} limit ${limit};`

                log.dbg(`sql:${query}`)

                let queryResult: any = await DbHelpers.dbQuery(client, query)

                const items = queryResult.rows.map(row => ({
                    sha: row.sha,
                    name: row.name,
                    mimeType: row.mimetype,
                    lastWrite: row.lastwrite * 1,
                    size: row.size * 1,
                    lat: row.latitude * 1,
                    lng: row.longitude * 1
                }))

                client.end()

                const resultDirectories = items.filter(i => i.mimeType == 'application/directory').map(({ sha, name }) => ({ sha, name }))
                const resultFiles = items.filter(i => i.mimeType != 'application/directory')

                res.send(JSON.stringify({ resultDirectories, resultFilesddd: resultFiles, items }))
            }
            catch (err) {
                res.send(JSON.stringify({ error: err }))
            }
        })
    }
}