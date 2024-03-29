import { LoggerBuilder } from '@ltearno/hexa-js'
import * as fs from 'fs'
import { DbConnectionParams } from "./Commands.js"
import * as DbHelpers from './DbHelpers.js'
import { HexaBackupStore, IHexaBackupStore } from "./HexaBackupStore.js"
import * as MimeTypes from './mime-types.js'
import * as Model from './Model.js'
import * as Operations from './Operations.js'
import * as SourceState from './SourceState.js'
//import { parseFile } from 'music-metadata'

const log = LoggerBuilder.buildLogger('db-index')

/* dynamically imported modules */
let exifParserBuilder: any = null

export async function updateObjectsIndex(store: HexaBackupStore, dbParams: DbConnectionParams) {
    log(`update objects index`)

    let client = null

    try {
        client = await DbHelpers.createClient(dbParams, "updateObjectsIndex")

        let sources = await store.getSources()
        for (let source of sources) {
            try {
                let sourceState = await store.getSourceState(source)
                if (!sourceState || !sourceState.currentCommitSha) {
                    log(`source ${source} is empty, skipping`)
                    continue
                }

                if (!SourceState.isIndexed(sourceState)) {
                    log(`source ${source} is not indexed, skipping`)
                    continue
                }

                log(`index from source ${source} commit ${sourceState.currentCommitSha}`)
                let commitSha = sourceState.currentCommitSha
                while (commitSha != null) {
                    let commit = await store.getCommit(commitSha)
                    if (!commit)
                        break

                    if (await DbHelpers.hasObjectHierarchy(client, commit.directoryDescriptorSha, source)) {
                        log(`skipped commit ${commitSha}, descriptor ${commit.directoryDescriptorSha} already indexed`)
                        break
                    }

                    // remove hierarchy records for the current source
                    await DbHelpers.deleteObjectsHierarchyFromSource(client, source)

                    if (commit.directoryDescriptorSha) {
                        await registerHierarchy("", client, store, source, commit.directoryDescriptorSha)
                    }

                    // mark commit as processed for the source
                    await DbHelpers.insertObjectHierarchy(client, source, "", {
                        isDirectory: true,
                        contentSha: commit.directoryDescriptorSha,
                        lastWrite: 0,
                        name: '',
                        size: 0
                    }, 'hexa-backup/x-hexa-backup-directory')

                    log(`commit ${commitSha} indexed`)

                    // do not go in commits depth, only index the current commit
                    commitSha = null
                    //commitSha = commit.parentSha
                }

                // index audio files
                {
                    const cursorClient = await DbHelpers.createClient(dbParams, "findUnindexedAudioFiles")
                    let cursor = await DbHelpers.createCursor(cursorClient, `select distinct(oh.sha) as sha from objects_hierarchy oh LEFT JOIN object_audio_tags oat ON oh.sha=oat.sha where mimeTypeType = 'audio' AND oat.sha IS NULL AND oh.sourceId='${source}';`)
                    while (true) {
                        try {
                            let rows = await cursor.read(100)
                            if (!rows || !rows.length) {
                                log(`no more rows`)
                                break
                            }
                            log(`we have ${rows.length} rows`)

                            for (let row of rows) {
                                let err = await processObjectAudioForSha(store, client, row.sha)
                                if (err)
                                    log.err(err)
                            }
                        }
                        catch (err) {
                            log.err(`error while reading cursor: ${err}`)
                            break
                        }
                    }
                    DbHelpers.closeClient(cursorClient)
                }

                // index image files
                {
                    const cursorClient = await DbHelpers.createClient(dbParams, "findUnindexedImageFiles")
                    let cursor = await DbHelpers.createCursor(cursorClient, `select distinct(oh.sha) as sha, mimeTypeSubType, lastWrite from objects_hierarchy oh LEFT JOIN object_exifs oe ON oh.sha=oe.sha where mimeTypeType='image' AND oh.size>0 AND oe.sha IS NULL AND oh.sourceId='${source}';`)
                    while (true) {
                        try {
                            let rows = await cursor.read(100)
                            if (!rows || !rows.length) {
                                log(`no more rows`)
                                break
                            }
                            log(`we have ${rows.length} rows`)

                            for (let row of rows) {
                                let err = await processObjectExifForSha(store, client, row.sha, row.mimetypesubtype, row.lastwrite)
                                if (err)
                                    log.err(err)
                            }
                        }
                        catch (err) {
                            log.err(`error while reading cursor: ${err}`)
                            break
                        }
                    }
                    DbHelpers.closeClient(cursorClient)
                }

            }
            catch (err) {
                log.err(`source ${source} had an error: ${err}, continuing`)
            }
        }

        await DbHelpers.dbQuery(client, `delete from object_exifs where sha in (select oe.sha from object_exifs oe left join objects_hierarchy o on oe.sha=o.sha where o.sha is null)`)
        await DbHelpers.dbQuery(client, `refresh materialized view audio_objects`)
    }
    catch (err) {
        console.error(err)
    }
    finally {
        DbHelpers.closeClient(client)
    }
}

async function registerHierarchy(path: string, client, store: HexaBackupStore, sourceId: string, directoryDescriptorSha: string) {
    log(`register hierarchy ${sourceId}, ${directoryDescriptorSha} ${path}`)

    // read the directory descriptor
    let dirDesc = await store.getDirectoryDescriptor(directoryDescriptorSha)
    if (!dirDesc) {
        log.err(`source '${sourceId}' cannot obtain directory descriptor for sha ${directoryDescriptorSha}`)
        return
    }

    // for each file, insert a record in objects_hierarchy
    for (let file of dirDesc.files) {
        if (!file.contentSha) {
            log.wrn(`source '${sourceId}', an entry in ${directoryDescriptorSha} has no contentSha (entry: ${JSON.stringify(file)})`)
            continue
        }

        const mimeType = getFileMimeType(file)

        await DbHelpers.insertObjectHierarchy(client, sourceId, directoryDescriptorSha, file, mimeType)

        if (file.isDirectory) {
            await registerHierarchy(`${path}/${file.name}`, client, store, sourceId, file.contentSha)
        }
    }
}

async function recPushDir(client, store: HexaBackupStore, basePath: string, directoryDescriptorSha: string, sourceId: string) {
    /* DELETED if (await DbHelpers.hasObjectSource(client, directoryDescriptorSha, sourceId)) {
        //log(`skipped ${directoryDescriptorSha} ${basePath}, already indexed`)
        return
    }*/

    log(`indexing directory descriptor ${directoryDescriptorSha} ${basePath}`)

    let dirDesc = await store.getDirectoryDescriptor(directoryDescriptorSha)
    if (!dirDesc) {
        log.err(`source '${sourceId}' cannot obtain directory descriptor for sha ${directoryDescriptorSha}`)
        return
    }

    log(`${dirDesc.files.length} files to push`)
    let nextTimeLog = Date.now() + 1000 * 10
    let nbPushed = 0

    for (let file of dirDesc.files) {
        nbPushed++
        if (Date.now() > nextTimeLog) {
            log(`still indexing ${directoryDescriptorSha} ${basePath} (${nbPushed}/${dirDesc.files.length})`)
            nextTimeLog = Date.now() + 1000 * 10
        }

        if (!file.contentSha) {
            if (!file.isDirectory)
                log.wrn(`source '${sourceId}', an entry in ${directoryDescriptorSha} has no contentSha (entry: ${JSON.stringify(file)})`)
            continue
        }

        if (file.isDirectory) {
            let path = `${basePath}${file.name.replace('\\', '/')}/`
            await recPushDir(client, store, path, file.contentSha, sourceId)
        }

        const mimeType = getFileMimeType(file)

        // DELETED await DbHelpers.insertObjectParent(client, file.contentSha, directoryDescriptorSha)
        // DELETED await DbHelpers.insertObjectSource(client, file.contentSha, sourceId)
        // DELETED await DbHelpers.insertObject(client, file, mimeType)

        if (false) {
            /* DELETED let err = await updateObjectAudioForSha(store, client, file, mimeType)
            if (err)
                log.err(err)
            let err = await updateObjectExifForSha(store, client, file, mimeType)
            if (err)
                log.err(err)
            await updateObjectFootprintForSha(client, file, mimeType)*/
        }
    }
}

function getFileMimeType(file: Model.FileDescriptor): string {
    if (file.isDirectory)
        return 'hexa-backup/x-hexa-backup-directory'

    let pos = file.name.lastIndexOf('.')
    if (pos >= 0) {
        let extension = file.name.substr(pos + 1).toLocaleLowerCase()
        if (extension in MimeTypes.MimeTypes)
            return MimeTypes.MimeTypes[extension]
    }

    return 'application/octet-stream'
}

async function updateObjectFootprintForSha(client: any, o: Model.FileDescriptor, mimeType: string) {
    let footprints = []

    let rs = await DbHelpers.dbQuery(client, { text: `select footprint from object_footprints where sha=$1`, values: [o.contentSha] })
    for (let row of rs.rows) {
        for (let f of row['footprint'].split(' ')) {
            if (!footprints.includes(f))
                footprints.push(f)
        }
    }

    // select all names of sha
    rs = await DbHelpers.dbQuery(client, {
        text: `select name from objects_hierarchy where sha=$1`,
        values: [o.contentSha]
    })
    for (let row of rs.rows) {
        if (!footprints.includes(row.name))
            footprints.push(row.name)
    }

    // if mimeType is audio/ => add artist, album and title
    if (mimeType && mimeType.startsWith('audio/')) {
        let rs = await DbHelpers.dbQuery(client, {
            text: `select tags#>>'{common,artist}' as artist, tags#>>'{common,album}' as album, tags#>>'{common,title}' as title, tags#>'{common,genre}'->>0 as genre from object_audio_tags where sha=$1`,
            values: [o.contentSha]
        })
        for (let row of rs.rows) {
            if (!footprints.includes(row.artist))
                footprints.push(row.artist)
            if (!footprints.includes(row.album))
                footprints.push(row.album)
            if (!footprints.includes(row.title))
                footprints.push(row.title)
            if (!footprints.includes(row.genre))
                footprints.push(row.genre)
        }
    }

    await DbHelpers.insertObjectFootprint(client, o.contentSha, footprints.join(' '))
}

let musicMetadata: any = null

async function processObjectAudioForSha(store: HexaBackupStore, client: any, sha: string): Promise<string> {
    let stage = `init`
    try {
        if (!musicMetadata) {
            stage = `requiring module`
            const m = await import('music-metadata')
            musicMetadata = m
            if (!musicMetadata)
                throw `cannot require/load module 'music-metadata'`
        }

        stage = `getShaFileName`
        let fileName = store.getShaFileName(sha)
        if (!fs.existsSync(fileName))
            throw `file does not exists: ${fileName}`

        stage = `parsing metadata for ${fileName}`
        let metadata = await musicMetadata.parseFile(fileName)
        if (!metadata)
            throw `no metadata for ${sha}`

        stage = `validating metadata ${JSON.stringify(metadata)}`
        metadata = JSON.parse(JSON.stringify(metadata))

        let artist = metadata?.common?.artist
        let album = metadata?.common?.album
        let title = metadata?.common?.title
        let genre = metadata?.common?.genre

        let footprint = [];
        [artist, album, title, genre].forEach(v => {
            if (!v)
                return

            if (Array.isArray(v))
                v = v.join(' ')

            let words = v.trim().split(' ')
            words.forEach(w => {
                if (v.length <= 3)
                    return
                if (!footprint.includes(w))
                    footprint.push(w)
            })
        })

        stage = `database insert`
        await DbHelpers.insertObjectAudioTags(client, sha, metadata, footprint.join(' '))
    }
    catch (err) {
        await DbHelpers.insertObjectAudioTags(client, sha, { error: err.toString() }, "")
        return `error update object audio sha ${sha} at stage ${stage}: ${err}`
    }

    return null
}

function parseDate(line) {
    try {
        line = line.trim()
        line = line.replace(/[^\x00-\x7F]/g, "")

        if (line.length == 0)
            return null

        // test if line has only digits
        if (line.match(/^\d+$/)) {
            let timestamp = parseInt(line)
            if (timestamp < 315532990) {
                return null
            }
            if (Math.abs(Date.now() - timestamp) < Math.abs(Date.now() - timestamp * 1000)) {
                //console.log(`ms ${timestamp}`);
            } else {
                //console.log(`s ${timestamp}`);
                timestamp *= 1000
            }
            if (timestamp > Date.now()) {
                return null
            }

            let date = new Date(timestamp)
            if ("" + date != "Invalid Date") {
                return date
            }
        }

        if (line.length == 10 && line[4] == ':' && line[7] == ':') {
            if (line.substring(0, 2) == "01")
                line = "20" + line.substring(2)
            return new Date(line.substring(0, 4), line.substring(5, 7) - 1, line.substring(8))
        }

        if (line.length > 10 && line[4] == ':' && line[7] == ':') {
            try {
                if (line.substring(0, 2) == "01")
                    line = "20" + line.substring(2)
                let hours = JSON.parse(line.substring(10))
                if (hours && hours.length == 3) {
                    return new Date(line.substring(0, 4), line.substring(5, 7) - 1, line.substring(8, 10), hours[0], hours[1], hours[2])
                }
            } catch (e) {
            }
        }

        let d = new Date(line)
        // test invalid date
        if ("" + d != "Invalid Date") {
            return d
        }

        var dateParts = line.split(/[:\s]+/);
        if (dateParts.length == 6) {
            let dd = new Date(dateParts[0], dateParts[1] - 1, dateParts[2], dateParts[3], dateParts[4], dateParts[5])
            if ("" + dd != "Invalid Date") {
                //console.log(`parsed ${line}: ${dd}`)
                return dd
            }
        }

        if (dateParts.length == 5) {
            let dd = new Date(dateParts[0], dateParts[1] - 1, dateParts[2], dateParts[3], dateParts[4], 0)
            if ("" + dd != "Invalid Date") {
                //console.log(`parsed ${line}: ${dd}`)
                return dd
            }
        }

        return null
    }
    catch (e) {
        return null
    }
}

async function processObjectExifForSha(store: HexaBackupStore, client: any, sha: string, mimeTypeSubType: string, lastWrite: string): Promise<string> {
    if (mimeTypeSubType == 'jpeg') {
        try {
            if (!exifParserBuilder) {
                const m = await import('exif-parser')
                exifParserBuilder = m
                if (!exifParserBuilder)
                    throw `cannot require/load module 'exif-parser'`
            }

            let size = await store.hasOneShaBytes(sha)

            let buffer = await store.readShaBytes(sha, 0, Math.min(size, 65635))
            if (!buffer)
                throw `cannot read 65kb from sha ${sha}`

            let exifParser = exifParserBuilder.create(buffer)
            let exif = exifParser.parse()

            log.dbg(`image size : ${JSON.stringify(exif.getImageSize())}`)
            log.dbg(`exif tags : ${JSON.stringify(exif.tags)}`)
            log.dbg(`exif thumbnail ? ${exif.hasThumbnail() ? 'yes' : 'no'}`)

            let dates = [
                lastWrite,
                `${exif.tags.CreateDate}`,
                `${exif.tags.DateTimeOriginal}`,
                `${exif.tags.GPSDateStamp || ''} ${exif.tags.GPSTimeStamp || ''}`,
            ].map(parseDate).filter(d => d != null).sort((a, b) => a.getTime() - b.getTime())

            // model + maker
            let model = (exif?.tags?.Model || '').trim()
            let maker = (exif?.tags?.Make || '').trim()
            if (!model.startsWith(maker))
                model = maker + ' ' + model
            let owner = (exif?.tags?.OwnerName || '').trim()
            if (owner.length > 0)
                model = model + ' (' + owner + ')'

            // width and height
            let height = exif?.tags?.ExifImageHeight || exif?.tags?.ImageHeight || null
            let width = exif?.tags?.ExifImageWidth || exif?.tags?.ImageWidth || null

            // latitude, latitude ref, longitude and longitude ref
            let latitude = exif?.tags?.GPSLatitude || null
            let latitudeRef = exif?.tags?.GPSLatitudeRef || null
            let longitude = exif?.tags?.GPSLongitude || null
            let longitudeRef = exif?.tags?.GPSLongitudeRef || null

            await DbHelpers.insertObjectExif(client, sha, exif.tags, mimeTypeSubType, dates.length ? dates[0] : null, model, height, width, latitude, latitudeRef, longitude, longitudeRef)
        }
        catch (err) {
            await DbHelpers.insertObjectExif(client, sha, { error: err.toString() }, mimeTypeSubType, parseDate(lastWrite), null, null, null, null, null, null, null)
            return `error processing image exif ${sha} : ${err}`
        }
    } else {
        await DbHelpers.insertObjectExif(client, sha, {}, mimeTypeSubType, parseDate(lastWrite), null, null, null, null, null, null, null)
    }

    return null
}

export async function updateMimeShaList(sourceId: string, mimeType: string, store: IHexaBackupStore, databaseParams: DbConnectionParams) {
    log(`store ready`)

    const client = await DbHelpers.createClient(databaseParams, "updateMimeShaList")

    log(`connected to database`)

    // TODO update after splitted mimeType into mimeTypeType and mimeTypeSubType
    const query = `select sha, min(distinct name) as name, min(size) as size, min(lastWrite) as lastWrite, min(mimeType) as mimeType from objects_hierarchy where size>100000 and mimeType ilike '${mimeType}/%' group by sha order by min(lastWrite);`

    const cursor = await DbHelpers.createCursor(client, query)

    let rootDirectoryDescriptor: Model.DirectoryDescriptor = { files: [] }
    let currentDirectoryDescriptor: Model.DirectoryDescriptor = { files: [] }
    let nbRows = 0

    function formatDate(date: Date) {
        var month = '' + (date.getMonth() + 1),
            day = '' + date.getDate(),
            year = date.getFullYear(),
            hour = '' + date.getHours(),
            minute = '' + date.getMinutes()

        if (month.length < 2) month = '0' + month;
        if (day.length < 2) day = '0' + day;
        if (hour.length < 2) hour = '0' + hour;
        if (minute.length < 2) minute = '0' + minute;

        return [year, month, day].join('-');
    }

    const maybePurge = async (max: number) => {
        if (!currentDirectoryDescriptor.files.length)
            return

        if (currentDirectoryDescriptor.files.length < max)
            return

        let pushedSha = await Operations.pushDirectoryDescriptor(currentDirectoryDescriptor, store)
        let date = formatDate(new Date(currentDirectoryDescriptor.files[0].lastWrite * 1))
        let dateEnd = formatDate(new Date(currentDirectoryDescriptor.files[currentDirectoryDescriptor.files.length - 1].lastWrite * 1))
        let desc = {
            contentSha: pushedSha,
            isDirectory: true,
            lastWrite: currentDirectoryDescriptor.files[currentDirectoryDescriptor.files.length - 1].lastWrite,
            name: `${date} à ${dateEnd} (${currentDirectoryDescriptor.files.length} photos)`,
            size: 0
        }
        rootDirectoryDescriptor.files.push(desc)
        log(`pushed ${desc.name}`)
        currentDirectoryDescriptor = { files: [] }
    }

    try {
        while (true) {
            let rows = await cursor.read(100)
            if (!rows || !rows.length) {
                log(`finished cursor`)
                await maybePurge(0)
                break
            }
            else {
                nbRows += rows.length
                log(`got ${nbRows} rows`)

                for (let row of rows) {
                    currentDirectoryDescriptor.files.push({
                        contentSha: row['sha'],
                        isDirectory: false,
                        lastWrite: parseInt(row['lastwrite']),
                        name: row['name'],
                        size: row['size']
                    })

                    await maybePurge(300)
                }
            }
        }

        let rootSha = await Operations.pushDirectoryDescriptor(rootDirectoryDescriptor, store)

        let commitSha = await store.registerNewCommit(sourceId, rootSha)
        log(`commited sha ${commitSha}, rootdesc ${rootSha}`)
    } catch (err) {
        log.err(`error parsing sql cursor : ${err}`)
    }

    await cursor.close()
    DbHelpers.closeClient(client)
}