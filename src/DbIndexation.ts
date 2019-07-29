import { LoggerBuilder } from '@ltearno/hexa-js'
import { IHexaBackupStore } from "./HexaBackupStore"
import { DbConnectionParams } from "./Commands"
import * as DbHelpers from './DbHelpers'
import * as Model from './Model'
import * as Operations from './Operations'

const log = LoggerBuilder.buildLogger('db-index')

export async function updateObjectsIndex(store: IHexaBackupStore, dbParams: DbConnectionParams) {
    log(`store ready`)

    const client = await DbHelpers.createClient(dbParams)

    let sources = await store.getSources()
    for (let source of sources) {
        try {
            log(`source ${source}`)
            let sourceState = await store.getSourceState(source)
            if (!sourceState || !sourceState.currentCommitSha)
                continue
            log(`commit ${sourceState.currentCommitSha}`)
            let commitSha = sourceState.currentCommitSha
            while (commitSha != null) {
                let commit = await store.getCommit(commitSha)
                if (!commit)
                    break

                if (commit.directoryDescriptorSha) {
                    // TODO if has object source, skip

                    await recPushDir(client, store, `${source}:`, commit.directoryDescriptorSha, source)

                    await DbHelpers.insertObject(client, { isDirectory: true, contentSha: commit.directoryDescriptorSha, lastWrite: 0, name: '', size: 0 })
                    await DbHelpers.insertObjectSource(client, commit.directoryDescriptorSha, source)
                }

                commitSha = commit.parentSha
            }
        }
        catch (err) {
            console.error(err)
        }
    }

    client.end()
}

async function recPushDir(client, store: IHexaBackupStore, basePath: string, directoryDescriptorSha, sourceId: string) {
    if (await DbHelpers.hasObjectSource(client, directoryDescriptorSha, sourceId)) {
        log(`skipped ${directoryDescriptorSha} ${basePath}, already indexed`)
        return
    }

    log(`pushing ${directoryDescriptorSha} ${basePath}`)

    let dirDesc = await store.getDirectoryDescriptor(directoryDescriptorSha)
    if (!dirDesc)
        return

    for (let file of dirDesc.files) {
        await DbHelpers.insertObjectParent(client, file.contentSha, directoryDescriptorSha)
        await DbHelpers.insertObjectSource(client, file.contentSha, sourceId)
        await DbHelpers.insertObject(client, file)

        if (file.isDirectory) {
            let path = `${basePath}${file.name.replace('\\', '/')}/`
            await recPushDir(client, store, path, file.contentSha, sourceId)
        }
    }
}

export async function updateExifIndex(store: IHexaBackupStore, databaseParams: DbConnectionParams) {
    log(`store ready`)

    const client = await DbHelpers.createClient(databaseParams)
    const client2 = await DbHelpers.createClient(databaseParams)

    log(`connected to database`)

    let baseQuery = `from objects o left join object_exifs op on o.sha=op.sha where size > 65635 and mimeType = 'image/jpeg' and (op.sha is null)`

    const queryCount = `select count(distinct o.sha) as total ${baseQuery};`
    let rs = await DbHelpers.dbQuery(client, queryCount)
    let nbTotal = rs.rows[0].total

    const query = `select distinct o.sha ${baseQuery};`

    const cursor = await DbHelpers.createCursor(client, query)

    let nbRows = 0
    let nbRowsError = 0
    log.setStatus(() => [`processed ${nbRows}/${nbTotal} rows so far (${nbRowsError} errors)`])

    let exifParserBuilder = require('exif-parser')

    try {
        while (true) {
            let rows = await cursor.read()
            if (!rows || !rows.length) {
                log(`finished cursor`)
                break
            }
            else {
                for (let row of rows) {
                    try {
                        let sha = row['sha']
                        log.dbg(`processing ${sha}`)
                        let buffer = await store.readShaBytes(sha, 0, 65635)
                        if (!buffer)
                            throw `cannot read 65kb from sha ${sha}`

                        let exifParser = exifParserBuilder.create(buffer)
                        let exif = exifParser.parse()

                        log.dbg(`image size : ${JSON.stringify(exif.getImageSize())}`)
                        log.dbg(`exif tags : ${JSON.stringify(exif.tags)}`)
                        log.dbg(`exif thumbnail ? ${exif.hasThumbnail() ? 'yes' : 'no'}`)

                        await DbHelpers.insertObjectExif(client2, sha, exif.tags)

                        nbRows++
                    }
                    catch (err) {
                        nbRowsError++
                        log.err(`error processing image ${row['sha']} : ${err}`)
                    }
                }
            }
        }
    } catch (err) {
        log.err(`error processing images : ${err}`)
    }

    log(`processed ${nbRows}/${nbTotal} images with ${nbRowsError} errors`)

    await cursor.close()

    client.end()
    client2.end()
}

export async function updateMimeShaList(sourceId: string, mimeType: string, store: IHexaBackupStore, databaseParams: DbConnectionParams) {
    log(`store ready`)

    const client = await DbHelpers.createClient(databaseParams)

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
    client.end()
}