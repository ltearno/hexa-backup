import { LoggerBuilder } from '@ltearno/hexa-js'
import { IHexaBackupStore, HexaBackupStore } from "./HexaBackupStore"
import { DbConnectionParams } from "./Commands"
import * as DbHelpers from './DbHelpers'
import * as Model from './Model'
import * as Operations from './Operations'
import * as MimeTypes from './mime-types'
import * as fs from 'fs'
import * as SourceState from './SourceState'

const log = LoggerBuilder.buildLogger('db-index')

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

                    if (commit.directoryDescriptorSha) {
                        // TODO if has object source, skip

                        try {
                            await recPushDir(client, store, `${source}:`, commit.directoryDescriptorSha, source)

                            await DbHelpers.insertObject(client, { isDirectory: true, contentSha: commit.directoryDescriptorSha, lastWrite: 0, name: '', size: 0 }, 'x-hexa-backup-directory')
                            await DbHelpers.insertObjectSource(client, commit.directoryDescriptorSha, source)
                        }
                        catch (err) {
                            log.err(`source ${source} had an error on commit ${commitSha} desc ${commit.directoryDescriptorSha}: ${err}, continuing`)
                        }
                    }

                    commitSha = commit.parentSha
                }
            }
            catch (err) {
                log.err(`source ${source} had an error: ${err}, continuing`)
            }
        }
    }
    catch (err) {
        console.error(err)
    }
    finally {
        DbHelpers.closeClient(client)
    }
}

async function recPushDir(client, store: HexaBackupStore, basePath: string, directoryDescriptorSha, sourceId: string) {
    if (await DbHelpers.hasObjectSource(client, directoryDescriptorSha, sourceId)) {
        //log(`skipped ${directoryDescriptorSha} ${basePath}, already indexed`)
        return
    }

    log(`pushing ${directoryDescriptorSha} ${basePath}`)

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

        await DbHelpers.insertObjectParent(client, file.contentSha, directoryDescriptorSha)
        await DbHelpers.insertObjectSource(client, file.contentSha, sourceId)
        await DbHelpers.insertObject(client, file, mimeType)
        await updateObjectAudioForSha(store, client, file.contentSha, mimeType)
    }
}

function getFileMimeType(file: Model.FileDescriptor) {
    if (file.isDirectory)
        return 'application/x-hexa-backup-directory'

    let pos = file.name.lastIndexOf('.')
    if (pos >= 0) {
        let extension = file.name.substr(pos + 1).toLocaleLowerCase()
        if (extension in MimeTypes.MimeTypes)
            return MimeTypes.MimeTypes[extension]
    }

    return 'application/octet-stream'
}

let musicMetadata: any = null

async function synchronizeRecord(title: string, baseQuery: string, store: HexaBackupStore, databaseParams: DbConnectionParams, callback: (sha: string, mimeType: string, row: any, store: HexaBackupStore, client: any) => Promise<any>) {
    log(`starting update of index ${title}`)

    const client = await DbHelpers.createClient(databaseParams, "synchronizeRecord")
    const client2 = await DbHelpers.createClient(databaseParams, "synchronizeRecord2")

    try {
        log(`connected to database`)

        const queryCount = `select count(distinct o.sha) as total ${baseQuery};`
        let rs = await DbHelpers.dbQuery(client, queryCount)
        let nbTotal = rs.rows[0].total

        let nbRows = 0
        let nbRowsError = 0

        const query = `select distinct o.sha, o.mimetype ${baseQuery};`

        const cursor = await DbHelpers.createCursor(client, query)

        while (true) {
            let rows = await cursor.read()
            if (!rows || !rows.length) {
                log(`finished cursor`)
                break
            }

            log(`processing batch of ${rows.length}`)

            for (let row of rows) {
                nbRows++
                let sha = row['sha']
                if (!sha)
                    continue

                log(`processing ${sha} (${nbRows}/${nbTotal} rows so far (${nbRowsError} errors))`)

                try {
                    await callback(sha, row['mimetype'], row, store, client2)
                }
                catch (err) {
                    nbRowsError++
                    log.err(`${title} error while processing ${sha} : ${err}`)
                }
            }
        }

        log(`processed ${nbRows}/${nbTotal} shas with ${nbRowsError} errors`)

        await cursor.close()

    } catch (err) {
        log.err(`error processing: ${err}`)
    } finally {
        DbHelpers.closeClient(client)
        DbHelpers.closeClient(client2)
    }


    log(`finished index update ${title}`)
}

export async function updateFootprintIndex(store: HexaBackupStore, databaseParams: DbConnectionParams) {
    log(`update footprint index`)

    await synchronizeRecord(`footprints`, `from objects o left join object_footprints of on o.sha=of.sha where (o.mimeType='application/x-hexa-backup-directory' or o.size > 65635) and (of.sha is null)`, store, databaseParams, async (sha, mimeType, row, store, client) => {
        let footprints = []

        // select all names of sha
        let rs = await DbHelpers.dbQuery(client, {
            text: `select name from objects where sha=$1`,
            values: [sha]
        })
        for (let row of rs.rows) {
            if (!footprints.includes(row.name))
                footprints.push(row.name)
        }

        // if mimeType is audio/ => add artist, album and title
        if (mimeType && mimeType.startsWith('audio/')) {
            let rs = await DbHelpers.dbQuery(client, {
                text: `select tags#>>'{common,artist}' as artist, tags#>>'{common,album}' as album, tags#>>'{common,title}' as title, tags#>'{common,genre}'->>0 as genre from object_audio_tags where sha=$1`,
                values: [sha]
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

        //if (footprints.length) {
        await DbHelpers.insertObjectFootprint(client, sha, footprints.join(' '))
        //}
    })
}

// returns an error if any
async function updateObjectAudioForSha(store: HexaBackupStore, client: any, sha: string, mimeType: string): Promise<string> {
    let stage = `init`
    try {
        if (!musicMetadata) {
            stage = `requiring module`
            musicMetadata = require('music-metadata')
            if (!musicMetadata)
                throw `cannot require/load module 'music-metadata'`
        }

        stage = `getShaFileName`
        let fileName = store.getShaFileName(sha)
        if (!fs.existsSync(fileName))
            throw `file does not exists: ${fileName}`

        stage = `readShaFile`
        let buffer = fs.readFileSync(fileName)
        if (!buffer || !buffer.length)
            throw `cannot read file ${fileName}`

        stage = `parsing metadata '${mimeType}' : ${sha} at ${fileName}`
        let metadata = await musicMetadata.parseBuffer(buffer, mimeType)
        if (!metadata)
            throw `no metadata for ${sha}`

        stage = `conforming metadata ${JSON.stringify(metadata)}`
        metadata = JSON.parse(JSON.stringify(metadata))

        stage = `database insert`
        await DbHelpers.insertObjectAudioTags(client, sha, metadata)
    }
    catch (err) {
        return `error update object audio sha ${sha} at stage ${stage}`
    }

    return null
}

let exifParserBuilder: any = null

export async function updateExifIndex(store: IHexaBackupStore, databaseParams: DbConnectionParams) {
    log(`update exif index`)

    const client = await DbHelpers.createClient(databaseParams, "updateExifIndex")
    const client2 = await DbHelpers.createClient(databaseParams, "updateExifIndex2")

    log(`connected to database`)

    let baseQuery = `from objects o left join object_exifs op on o.sha=op.sha where size > 65635 and mimeType = 'image/jpeg' and (op.sha is null)`

    const queryCount = `select count(distinct o.sha) as total ${baseQuery};`
    let rs = await DbHelpers.dbQuery(client, queryCount)
    let nbTotal = rs.rows[0].total

    const query = `select distinct o.sha ${baseQuery};`

    const cursor = await DbHelpers.createCursor(client, query)

    let nbRows = 0
    let nbRowsError = 0

    if (!exifParserBuilder)
        exifParserBuilder = require('exif-parser')

    try {
        while (true) {
            let rows = await cursor.read()
            if (!rows || !rows.length) {
                log(`finished cursor`)
                break
            }

            for (let row of rows) {
                nbRows++
                let sha = row['sha']
                if (!sha)
                    continue

                log(`processing ${sha} (${nbRows}/${nbTotal} rows so far (${nbRowsError} errors))`)

                try {
                    // insert an empty object in case the job stales, so it does not come up again...
                    await DbHelpers.insertObjectExif(client2, sha, {})

                    let buffer = await store.readShaBytes(sha, 0, 65635)
                    if (!buffer)
                        throw `cannot read 65kb from sha ${sha}`

                    let exifParser = exifParserBuilder.create(buffer)
                    let exif = exifParser.parse()

                    log.dbg(`image size : ${JSON.stringify(exif.getImageSize())}`)
                    log.dbg(`exif tags : ${JSON.stringify(exif.tags)}`)
                    log.dbg(`exif thumbnail ? ${exif.hasThumbnail() ? 'yes' : 'no'}`)

                    await DbHelpers.insertObjectExif(client2, sha, exif.tags)
                }
                catch (err) {
                    nbRowsError++
                    log.err(`error processing image ${sha} : ${err}`)
                }

                //log(`endp`)
            }
        }
    } catch (err) {
        log.err(`error processing images : ${err}`)
    }

    log(`processed ${nbRows}/${nbTotal} images with ${nbRowsError} errors`)

    await cursor.close()

    DbHelpers.closeClient(client)
    DbHelpers.closeClient(client2)
}

export async function updateMimeShaList(sourceId: string, mimeType: string, store: IHexaBackupStore, databaseParams: DbConnectionParams) {
    log(`store ready`)

    const client = await DbHelpers.createClient(databaseParams, "updateMimeShaList")

    log(`connected to database`)

    const query = `select sha, min(distinct name) as name, min(size) as size, min(lastWrite) as lastWrite, min(mimeType) as mimeType from objects where size>100000 and mimeType ilike '${mimeType}/%' group by sha order by min(lastWrite);`

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
            let rows = await cursor.read()
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