import { LoggerBuilder } from '@ltearno/hexa-js'
import * as http from 'http'
import * as Authorization from '../Authorization.js'
import * as BackgroundJobs from '../BackgroundJobs.js'
import * as DbHelpers from '../DbHelpers.js'
import * as DbIndexation from '../DbIndexation.js'
import { HexaBackupStore } from '../HexaBackupStore.js'
import * as SourceState from '../SourceState.js'

const log = LoggerBuilder.buildLogger('stateful-server')

const SQL_RESULT_LIMIT = 162

interface EnumeratedPath {
    name: string
    sha: string
    parentSha: string
    sourceId: string
    parents: EnumeratedPath[]
}

async function browsePathsToSha(client: any, authorizedRefs: string, sha: string): Promise<EnumeratedPath[]> {
    let query = `select o.sourceId, o.sha, o.name, o.parentSha from objects_hierarchy o where o.sourceId in (${authorizedRefs}) and o.sha = '${sha}'`
    let queryResult: any = await DbHelpers.dbQuery(client, query)
    let result = []
    for (let row of queryResult.rows) {
        const parentSha = row.parentsha.trim()
        result.push({
            name: row.name,
            sha: row.sha,
            parentSha,
            sourceId: row.sourceid,
            parents: await browsePathsToSha(client, authorizedRefs, parentSha)
        })
    }

    return result
}

interface PartInfo {
    name: string
    sha: string
}
interface PathInfo {
    sourceId: string
    parts: PartInfo[]
}

async function enumeratePathsToShaInternal(paths: EnumeratedPath[], parts: PartInfo[], pathInfos: PathInfo[]) {
    paths.forEach(path => {
        let subParts = parts.slice()
        if (!path.parents.length) {
            pathInfos.push({
                sourceId: path.sourceId,
                parts: subParts.reverse().map(a => ({ name: a.name, sha: a.sha }))
            })
        } else {
            subParts.push(path)
            enumeratePathsToShaInternal(path.parents, subParts, pathInfos)
        }
    })
}

async function enumeratePathsToSha(client: any, authorizedRefs: string, sha: string): Promise<PathInfo[]> {
    let infos = []
    await enumeratePathsToShaInternal(await browsePathsToSha(client, authorizedRefs, sha), [], infos)
    return infos
}

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

    private async searchImages(offset: number | null, limit: number | null) {
        const client = await DbHelpers.createClient(this.databaseParams, "searchimages")
        offset = offset || 0
        limit = limit || SQL_RESULT_LIMIT

        try {
            let count = (await DbHelpers.dbQuery(client, `select count(*) as total from object_exifs`)).rows[0].total * 1
            let res = await DbHelpers.dbQuery(client, `select oe.sha, oe.date, oe.model from object_exifs oe order by oe.date, oe.model, oe.sha offset ${offset} limit ${limit}`)

            return {
                count: count,
                items: res.rows.map(row => ({
                    sha: row.sha,
                    date: row.date,
                    model: row.model
                }))
            }
        }
        catch (e) {
            return { error: e }
        }
        finally {
            DbHelpers.closeClient(client)
        }

        return { error: "unknown request" }
    }

    private async searchAudio(authorizedRefs: string, names: string[]) {
        const offset = 0
        const limit = SQL_RESULT_LIMIT
        const client = await DbHelpers.createClient(this.databaseParams, "searchaudio")

        try {
            let conditions: string[] = [`o.sourceId in (${authorizedRefs})`]
            names.forEach(name => {
                conditions.push(`o.footprint ilike '%${name}%'`)
            })

            // find in directories
            let query = `select o.sha, o.name, o.parentSha as parentSha, o.name, o.mimeTypeType, o.mimeTypeSubType from objects_hierarchy o where o.sourceId in (${authorizedRefs}) and mimeTypeType='hexa-backup' and mimeTypeSubType='x-hexa-backup-directory' and (${names.map(n => `o.name ilike '%${n}%'`).join(' and ')}) order by (${names.map(n => `similarity(o.name, '${n}')`).join(' + ')}) desc limit ${limit} offset ${offset};`
            log(query)
            let queryResult: any = await DbHelpers.dbQuery(client, query)
            let directories = queryResult.rows.map(row => {
                return {
                    sha: row.sha,
                    name: row.name,
                    mimeType: row.mimetypetype.trim() + '/' + row.mimetypesubtype.trim(),
                    score: 0,
                    parentSha: row.parentsha
                }
            })
            log(directories)

            // find audio_objects
            query = `select o.sha, o.name, o.parentSha as parentSha, o.name, o.mimeTypeType, o.mimeTypeSubType from audio_objects o where o.sourceId in (${authorizedRefs}) and (${names.map(n => `o.footprint ilike '%${n}%'`).join(' and ')}) order by (${names.map(n => `similarity(o.footprint, '${n}')`).join(' + ')}) desc limit ${limit} offset ${offset};`
            log(query)
            queryResult = await DbHelpers.dbQuery(client, query)
            let files = queryResult.rows.map(row => {
                return {
                    sha: row.sha,
                    name: row.name,
                    mimeType: row.mimetypetype.trim() + '/' + row.mimetypesubtype.trim(),
                    score: 0,
                    parentSha: row.parentsha
                }
            })
            log(files)

            let directoryMentions = {}
            files.forEach(f => {
                if (!directoryMentions[f.parentSha])
                    directoryMentions[f.parentSha] = 0
                directoryMentions[f.parentSha]++
            })

            directories.forEach(d => directoryMentions[d.sha] = true)
            //files = files.filter(f => !directoryMentions[f.sha])

            // add directories which are mentionned more than x times as file's parent
            const mentionTrigger = 2
            let directoryShasToAdd = []
            // if a directory has more than mentionTrigger mentions, it is added to the resultDirectories and corresponding files are removed from resultFiles
            Object.keys(directoryMentions).forEach(sha => {
                if (directoryMentions[sha] >= mentionTrigger) {
                    //resultFiles = resultFiles.filter(f => f.parentSha != sha)
                    directoryShasToAdd.push(sha)
                }
            })
            if (directoryShasToAdd.length > 0) {
                let query = `select sha, name, mimeTypeType, mimeTypeSubType, parentSha from objects_hierarchy where sha in ('${directoryShasToAdd.join("','")}')`
                log(`sql - 1:${query}`)
                let queryResult: any = await DbHelpers.dbQuery(client, query)
                let directoriesToAdd = queryResult.rows.map(row => {
                    return {
                        sha: row.sha,
                        name: row.name,
                        mimeType: row.mimetypetype.trim() + '/' + row.mimetypesubtype.trim(),
                        parentSha: row.parentsha,
                    }
                })
                log("added directories" + directoriesToAdd)
                directories = directories.concat(directoriesToAdd)
            }

            // remove duplicate directories and files
            directories = directories.filter((d, i) => directories.findIndex(d2 => d2.sha == d.sha) == i)
            files = files.filter((f, i) => files.findIndex(f2 => f2.sha == f.sha) == i)

            // remove directories which have no audio inside
            let directoryShas = directories.map(d => d.sha)
            query = `select parentSha from objects_hierarchy where mimeTypeType = 'audio' and parentSha in ('${directoryShas.join("','")}') group by parentSha`
            log(`sql - 2:${query}`)
            queryResult = await DbHelpers.dbQuery(client, query)
            let directoryShasWithAudio = queryResult.rows.map(row => row.parentsha)
            directories = directories.filter(d => directoryShasWithAudio.find(sha => sha == d.sha))

            // sort by name
            directories.sort((a, b) => a.name.localeCompare(b.name))
            files.sort((a, b) => a.name.localeCompare(b.name))

            return { directories, files }
        }
        catch (err) {
            log.err(`exception during searchAudio: ${err}`)
            return { error: `see server logs !` }
        }
        finally {
            DbHelpers.closeClient(client)
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

                const query = `select distinct o.parentSha from objects_hierarchy o where ${authorizedRefs != null ?
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
                errors: [],
                paths: [],
            }

            try {
                info.paths = await enumeratePathsToSha(client, authorizedRefs, sha)
            }
            catch (err) {
                info.errors.push(err)
            }

            try {
                let queryResult: any = await DbHelpers.dbQuery(client, {
                    text: `select * from objects_hierarchy where sha=$1 and sourceId in (${authorizedRefs})`,
                    values: [sha]
                })

                queryResult.rows.forEach(row => {
                    if (!info.names.includes(row.name))
                        info.names.push(row.name)
                    const mimeType = row.mimetypetype + '/' + row.mimetypesubtype
                    if (!info.mimeTypes.includes(mimeType))
                        info.mimeTypes.push(mimeType)
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
                    text: `select parentsha from objects_hierarchy where sha=$1 and sourceId in (${authorizedRefs})`,
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
                    text: `select sourceId from objects_hierarchy where sha=$1 and sourceId in (${authorizedRefs})`,
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
                    if (row.tags)
                        delete row.tags['quality']
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
            let authorizedRefs = await Authorization.getAuthorizedRefsFromHttpRequestAsSql(req, this.store)
            if (!authorizedRefs) {
                res.send(JSON.stringify({ directories: [], files: [] }))
                return
            }

            const client = await DbHelpers.createClient(this.databaseParams, "search")

            let query = '-'
            res.set('Content-Type', 'application/json')
            try {
                let {
                    name,
                    mimeType,
                    geoSearch,
                    dateMin,
                    dateMax,
                    offset,
                    limit,
                    type,
                } = req.body

                if (type == 'image') {
                    let results = await this.searchImages(offset, limit)
                    res.send(JSON.stringify(results))
                    return
                }

                let names = (name || '').split(' ').map(n => n.trim()).filter(n => n != '')

                if (mimeType.startsWith("audio/")) {
                    let results = await this.searchAudio(authorizedRefs, names)
                    res.send(JSON.stringify(results))
                    return
                }

                let selects: string[] = ['o.sha', 'o.name', 'o.mimeTypeType', 'o.mimeTypeSubType', 'o.parentSha as parentSha']
                let whereConditions: string[] = [`o.sourceId in (${authorizedRefs})`]
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

                    if (dateMin)
                        whereConditions.push(`o.lastWrite>=${dateMin}`)
                    if (dateMax)
                        whereConditions.push(`o.lastWrite<=${dateMax}`)
                }

                if (mimeType && mimeType.startsWith('image/') && dateMin && dateMax) {
                    froms.push(`inner join object_exifs oe on o.sha=oe.sha`)
                    if (dateMin)
                        whereConditions.push(`oe.date>='${new Date(dateMin).toISOString().slice(0, 16).replace('T', ' ')}'`)
                    if (dateMax)
                        whereConditions.push(`oe.date<='${new Date(dateMax).toISOString().slice(0, 16).replace('T', ' ')}'`)
                }

                if (!name)
                    name = ''
                name = name.trim()
                if (name != '') {
                    if (mimeType && mimeType.startsWith('audio/')) {
                        froms.push(`left join object_audio_tags oat on o.sha=oat.sha`)

                        whereConditions.push(`(${names.map(name => `(o.name ilike '%${name}%' OR oat.footprint ilike '%${name}%')`).join(' AND ')})`)

                        let similarities = names.map(name => `similarity(o.name, '${name}') + similarity(oat.footprint, '${name}')`).join(' + ')
                        orders.push(`order by (${similarities}) desc`)
                        selects.push(`(${similarities}) as score`)
                    }
                    else {
                        whereConditions.push(`o.name % '${name}' or o.name ilike '%${name}%'`)
                        orders.push(`order by similarity(o.name, '${name}') desc`)
                        selects.push(`similarity(o.name, '${name}') as score`)
                    }
                }

                // TODO not only audio !!!
                if (mimeType && mimeType.startsWith('audio/')) {
                    whereConditions.push(`(o.mimeTypeType = 'hexa-backup' and o.mimeTypeSubType = 'x-hexa-backup-directory') or o.mimeTypeType = 'audio'`)
                }

                if (!offset || offset < 0)
                    offset = 0
                else
                    offset = Math.floor(offset * 1)

                if (!limit || limit < 1 || limit > SQL_RESULT_LIMIT)
                    limit = SQL_RESULT_LIMIT

                query = `select ${selects.join(', ')} from ${froms.join(' ')} where ${whereConditions.map(c => `(${c})`).join(' and ')} ${orders.join(', ')} limit ${limit} offset ${offset};`

                log(`sql:${query}`)

                let queryResult: any = await DbHelpers.dbQuery(client, query)

                const items = queryResult.rows.map(row => {
                    return {
                        sha: row.sha,
                        name: row.name,
                        mimeType: row.mimetypetype.trim() + '/' + row.mimetypesubtype.trim(),
                        score: row.score * 1,
                        parentSha: row.parentsha,

                        lat: row.latitude * 1,
                        lng: row.longitude * 1,

                        title: row.title,
                        artist: row.artist,
                        album: row.album,
                    }
                })

                let resultDirectories = items.filter(i => i.mimeType == 'hexa-backup/x-hexa-backup-directory').map(({ sha, name }) => ({ sha, name }))
                let resultFiles = items.filter(i => i.mimeType != 'hexa-backup/x-hexa-backup-directory')

                // remove files which are child of a directory
                resultFiles = resultFiles.filter(f => !resultDirectories.find(d => f.parentSha == d.sha))

                // files that have the same parentSha are grouped together
                let directoryMentions = {}
                resultFiles.forEach(f => {
                    if (!directoryMentions[f.parentSha])
                        directoryMentions[f.parentSha] = 0
                    directoryMentions[f.parentSha]++
                })
                //log(`directoryMentions:${JSON.stringify(directoryMentions)}`)
                const mentionTrigger = 2
                let directoryShasToAdd = []
                // if a directory has more than mentionTrigger mentions, it is added to the resultDirectories and corresponding files are removed from resultFiles
                Object.keys(directoryMentions).forEach(sha => {
                    if (directoryMentions[sha] >= mentionTrigger) {
                        //resultFiles = resultFiles.filter(f => f.parentSha != sha)
                        directoryShasToAdd.push(sha)
                    }
                })
                //log(`directoryShasToAdd:${JSON.stringify(directoryShasToAdd)}`)
                if (directoryShasToAdd.length > 0) {
                    let query = `select sha, name, mimeTypeType, mimeTypeSubType, parentSha from objects_hierarchy where sha in ('${directoryShasToAdd.join("','")}')`
                    log(`sql - 1:${query}`)
                    let queryResult: any = await DbHelpers.dbQuery(client, query)
                    let directoriesToAdd = queryResult.rows.map(row => {
                        return {
                            sha: row.sha,
                            name: row.name,
                            mimeType: row.mimetypetype.trim() + '/' + row.mimetypesubtype.trim(),
                            score: row.score * 1,
                            parentSha: row.parentsha,

                            lat: row.latitude * 1,
                            lng: row.longitude * 1,

                            title: row.title,
                            artist: row.artist,
                            album: row.album,
                        }
                    })
                    //log(directoriesToAdd)
                    resultDirectories = resultDirectories.concat(directoriesToAdd)
                }

                // remove items from resultDirectories that have the same sha as another item in resultDirectories
                resultDirectories = resultDirectories.filter((d, i) => resultDirectories.findIndex(d2 => d2.sha == d.sha) == i)
                // same for files
                resultFiles = resultFiles.filter((f, i) => resultFiles.findIndex(f2 => f2.sha == f.sha) == i)

                // remove directories which have no audio inside
                if (mimeType && mimeType.startsWith('audio/')) {
                    let directoryShas = resultDirectories.map(d => d.sha)
                    let query = `select parentSha from objects_hierarchy where mimeTypeType = 'audio' and parentSha in ('${directoryShas.join("','")}') group by parentSha`
                    log(`sql - 2:${query}`)
                    let queryResult: any = await DbHelpers.dbQuery(client, query)
                    let directoryShasWithAudio = queryResult.rows.map(row => row.parentsha)
                    resultDirectories = resultDirectories.filter(d => directoryShasWithAudio.find(sha => sha == d.sha))
                }

                res.send(JSON.stringify({ directories: resultDirectories, files: resultFiles, items: [], query: '' }))
            }
            catch (err) {
                log.err(err)
                res.send(JSON.stringify({ error: err, query }))
            }
            finally {
                DbHelpers.closeClient(client)
            }
        })
    }
}
