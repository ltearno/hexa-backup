import { HexaBackupStore } from '../HexaBackupStore'
import { LoggerBuilder } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization'
import * as DbHelpers from '../DbHelpers'

const log = LoggerBuilder.buildLogger('stateful-server')

export class Stateful {
    constructor(private store: HexaBackupStore, private databaseParams: DbHelpers.DbParams) {
        this.store.addCommitListener((commitSha, sourceId) => { })
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
                        ''} o.sha = '${sha}' limit 500;`

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
                    `inner join object_sources os on o.parentsha=os.sha` :
                    ``} where ${authorizedRefs != null ?
                        `os.sourceId in (${authorizedRefs}) and` :
                        ''} o.sha = '${sha}' limit 500;`

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

                let { name, mimeType, geoSearch, dateMin, dateMax } = req.body

                const client = await DbHelpers.createClient(this.databaseParams)

                let query = `select o.sha, o.name from objects o ${authorizedRefs !== null ?
                    `inner join object_sources os on o.sha=os.sha` :
                    ``} where ${authorizedRefs != null ?
                        `os.sourceId in (${authorizedRefs}) and` :
                        ''} (o.name % '${name}' or o.name ilike '%${name}%') and o.isDirectory group by o.sha, o.name order by similarity(o.name, '${name}') desc limit 500;`

                log.dbg(`sql:${query}`)

                let resultDirectories: any = name != '' ? await DbHelpers.dbQuery(client, query) : { rows: [] }
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

                query = `select o.sha, o.name, o.mimeType${geoSearchSelect}, min(o.size) as size, min(o.lastWrite) as lastWrite from objects o ${authorizedRefs !== null ?
                    `inner join object_sources os on o.sha=os.sha` :
                    ``}${geoSearchJoin} where ${authorizedRefs !== null ?
                        `os.sourceId in (${authorizedRefs})` :
                        '1=1'}${nameWhere} and o.mimeType != 'application/directory' and o.mimeType like '${mimeType}'${geoSearchWhere}${dateWhere} group by o.sha, o.name, o.mimeType${geoSearchGroupBy} ${orderBy} limit 500;`

                log.dbg(`sql:${query}`)

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
    }
}