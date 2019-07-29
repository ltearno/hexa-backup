import * as ShaCache from './ShaCache'
import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
import { HashTools, FsTools, LoggerBuilder, Queue, OrderedJson } from '@ltearno/hexa-js'
import * as Model from './Model'
import * as fs from 'fs'
import * as path from 'path'
import * as DirectoryBrowser from './DirectoryBrowser'
import * as ClientPeering from './ClientPeering'
import * as Tools from './Tools'
import * as Operations from './Operations'
import * as WebServer from './WebServer'
import * as DbHelpers from './DbHelpers'

const log = LoggerBuilder.buildLogger('Commands')

export interface StoreConnectionParams {
    host: string
    port: number
    token: string
    insecure: boolean
}

export interface DbConnectionParams {
    host: string
    database: string
    user: string
    password: string
    port: number
}

interface TreeDirectoryInfo {
    files: Model.FileDescriptor[]
    name: string
    lastWrite: number
    directories: TreeDirectoryInfo[]
}

export async function refs(storeParams: StoreConnectionParams, _verbose: boolean) {
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    console.log(`refs on store`)
    console.log()

    let refs = await store.getRefs()

    if (refs == null) {
        console.log(`refs not found !`)
        return
    }

    for (let ref of refs) {
        console.log(`${ref}`)
    }
}


export async function sources(storeParams: StoreConnectionParams, verbose) {
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    console.log(`sources on store`)

    let sources = await store.getSources()

    if (sources == null) {
        console.log()
        console.log(`refs not found !`)
        return
    }

    for (let sourceId of sources) {
        verbose && console.log()
        console.log(`${sourceId}`)
        if (!verbose)
            continue

        try {
            let state = await store.getSourceState(sourceId)
            if (state.currentCommitSha) {
                console.log(` current commit sha : ${state.currentCommitSha.substr(0, 7)}`)
                let commitSha = state.currentCommitSha

                let currentCommit = await store.getCommit(commitSha)
                if (!currentCommit) {
                    console.log(`  commit ${commitSha} not found !`)
                }
                else {
                    let currentDirectoryDescriptor = await store.getDirectoryDescriptor(currentCommit.directoryDescriptorSha)
                    if (!currentDirectoryDescriptor) {
                        console.log(`  descriptor ${currentCommit.directoryDescriptorSha} not found !`)
                    }
                    else {
                        let payload = JSON.stringify(currentDirectoryDescriptor)

                        console.log(` nb descriptor items : ${currentDirectoryDescriptor.files.length}`)
                        console.log(` descriptor size     : ${Tools.prettySize(payload.length)}`)

                        console.log(` commit history      :`)
                        while (commitSha != null) {
                            let commit = await store.getCommit(commitSha)
                            if (commit == null) {
                                console.log(`  error : commit ${commitSha} not found !`)
                                break
                            }

                            console.log(`  ${Tools.displayDate(commit.commitDate)} commit ${commitSha.substr(0, 7)} desc ${commit.directoryDescriptorSha.substr(0, 7)}`)

                            commitSha = commit.parentSha
                        }
                    }
                }
            }
        }
        catch (err) {
            console.log(` error ! ${JSON.stringify(err)}`)
        }
    }
}

export async function stats(storeParams: StoreConnectionParams) {
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    console.log(`stats on store`)

    let stats = await store.stats()

    if (stats == null) {
        console.log()
        console.log(`stats not found !`)
        return
    }

    log(JSON.stringify(stats, null, 4))
}


export async function history(sourceId: string, storeParams: StoreConnectionParams, verbose: boolean) {
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    console.log(`history of ${sourceId} in store`)
    console.log()

    let sourceState = await store.getSourceState(sourceId);

    if (sourceState == null) {
        console.log(`source state not found !`)
        return
    }

    let directoryDescriptorShaToShow = null
    let commitSha = sourceState.currentCommitSha
    if (commitSha == null)
        console.log('empty !')

    while (commitSha != null) {
        let commit = await store.getCommit(commitSha)
        if (commit == null) {
            console.log(`error : commit ${commitSha} not found !`)
            break
        }

        console.log(`${Tools.displayDate(commit.commitDate)}`)
        console.log(` commit: ${commitSha}`)
        console.log(` desc:   ${commit.directoryDescriptorSha}`)
        console.log('')

        if (directoryDescriptorShaToShow == null)
            directoryDescriptorShaToShow = commit.directoryDescriptorSha

        commitSha = commit.parentSha
    }

    if (verbose && directoryDescriptorShaToShow) {
        console.log()
        console.log(`most recent commit's directory structure (${directoryDescriptorShaToShow}) :`)
        let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorShaToShow)
        await showDirectoryDescriptor(directoryDescriptor, store)
    }
}

async function loadTreeDirectoryInfoFromDirectoryDescriptor(_store: IHexaBackupStore, directoryDescriptor: Model.DirectoryDescriptor): Promise<TreeDirectoryInfo> {
    let rootDirectory: TreeDirectoryInfo = {
        name: '',
        lastWrite: 0,
        files: [],
        directories: []
    }

    let getDirectory = (parts: string[]) => {
        let cur = rootDirectory

        for (let i = 0; i < parts.length; i++) {
            let child = cur.directories.find(c => c.name == parts[i])
            if (!child) {
                child = {
                    name: parts[i],
                    lastWrite: 0,
                    directories: [],
                    files: []
                }
                cur.directories.push(child)
            }

            cur = child
        }

        return cur
    }

    let dirAndBase = (p: string): [string[], string] => {
        let parts = p.split(/[\/\\]/)
        let base = parts.pop()
        return [parts, base]
    }

    directoryDescriptor.files.forEach(d => {
        let [dir, base] = dirAndBase(d.name)
        let baseDirectory = getDirectory(dir)

        if (d.isDirectory) {
            if (!d.contentSha || d.contentSha.trim() == '') {
                let directory = getDirectory(d.name.split(/[\/\\]/))
                directory.lastWrite = d.lastWrite
            }
            else {
                // copy existing
                baseDirectory.files.push({
                    name: base,
                    contentSha: d.contentSha,
                    isDirectory: true,
                    lastWrite: d.lastWrite,
                    size: d.size
                })
            }
        }
        else {
            // copy existing
            baseDirectory.files.push({
                name: base,
                contentSha: d.contentSha,
                isDirectory: false,
                lastWrite: d.lastWrite,
                size: d.size
            })
        }
    })

    return rootDirectory
}

export async function normalize(sourceId: string, storeParams: StoreConnectionParams) {
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    log(`normalize source ${sourceId}`)

    let state = await store.getSourceState(sourceId)
    if (!state) {
        log.err(`cannot get remote source state`)
        return
    }

    if (!state.currentCommitSha) {
        log.err(`remote has no commit (${sourceId})`)
        return
    }

    let currentCommit = await store.getCommit(state.currentCommitSha)
    if (!currentCommit) {
        log.err(`cannot get remote commit ${state.currentCommitSha}`)
        return
    }

    let startSha = currentCommit.directoryDescriptorSha

    let directoryDescriptor = await store.getDirectoryDescriptor(startSha)
    log(`actual descriptor: ${startSha} ${directoryDescriptor.files.length} files`)

    let rootDirectory = await loadTreeDirectoryInfoFromDirectoryDescriptor(store, directoryDescriptor)

    let shasToSend = new Map<string, Buffer>()

    // browse the tree structure and register each new directory
    let hier2Flat = (d: TreeDirectoryInfo): Model.DirectoryDescriptor => {
        let out: Model.DirectoryDescriptor = {
            files: []
        }

        d.directories.forEach(subDir => {
            let subDirDescriptor = hier2Flat(subDir)
            let stringified = OrderedJson.stringify(subDirDescriptor)
            let subDirDescriptorRaw = Buffer.from(stringified, 'utf8')
            let subDirDescriptorSha = HashTools.hashStringSync(stringified)

            shasToSend.set(subDirDescriptorSha, subDirDescriptorRaw)

            out.files.push({
                name: subDir.name,
                isDirectory: true,
                lastWrite: subDir.lastWrite,
                size: subDirDescriptorRaw.length,
                contentSha: subDirDescriptorSha
            })
        })

        out.files = out.files.concat(d.files)

        return out
    }

    let rootDescriptor = hier2Flat(rootDirectory)
    let stringified = OrderedJson.stringify(rootDescriptor)
    let rootDescriptorRaw = Buffer.from(stringified, 'utf8')
    let rootDescriptorSha = HashTools.hashStringSync(stringified)
    shasToSend.set(rootDescriptorSha, rootDescriptorRaw)
    log(`   new descriptor: ${rootDescriptorSha} ${rootDescriptor.files.length} files`)

    let i = 0
    for (let [sha, content] of shasToSend) {
        i++
        let len = await store.hasOneShaBytes(sha)
        if (len != content.length) {
            log(`send directory ${i}/${shasToSend.size} ${sha}`)
            await store.putShaBytes(sha, 0, content)
            let ok = await store.validateShaBytes(sha)
            if (!ok)
                log.err(`sha not validated ${sha}`)
        }
        else {
            log(`directory already in store ${i}/${shasToSend.size} ${sha}`)
        }
    }

    let result = await store.registerNewCommit(sourceId, rootDescriptorSha)
    log(`finished normalization: ${result}`)
}

async function parseSourceSpec(s: string, store: IHexaBackupStore) {
    if (!s || !s.trim().length) {
        log.err(`no source`)
        return null
    }

    if (!s.includes(':')) {
        // assume a sha
        return s
    }
    else {
        let parts = s.split(':')
        if (parts.length != 2) {
            log.err(`syntax error for target : '${s}'`)
            return null
        }

        let sourceId = parts[0]

        let path = parts[1].trim()
        if (path.startsWith('/'))
            path = path.substring(1)
        let pathParts = path.length ? path.split('/') : []

        let sourceState = await store.getSourceState(sourceId)
        if (!sourceState) {
            log.err(`cannot find ${sourceId} source state`)
            return null
        }

        let commit = await store.getCommit(sourceState.currentCommitSha)
        if (!commit || !commit.directoryDescriptorSha) {
            log.err(`cannot find commit ${sourceState.currentCommitSha}, or commit has no descriptor`)
            return null
        }

        let current = commit.directoryDescriptorSha

        for (let subDirectoryName of pathParts) {
            let currentDesc = await store.getDirectoryDescriptor(current)
            if (!currentDesc) {
                log.err(`cannot load descriptor ${current}`)
                return null
            }

            let foundSubDirectory = currentDesc.files.find(item => item.name == subDirectoryName)
            if (!foundSubDirectory) {
                log.err(`cannot find sub path '${subDirectoryName}'`)
                return null
            }

            current = foundSubDirectory.contentSha
        }

        return current
    }
}

export async function copy(sourceId: string, pushedDirectory: string, destination: string, recursive: boolean, storeParams: StoreConnectionParams) {
    log('connecting to remote store...')
    log(`copy (push+merge) options :`)
    log(`  directory: ${pushedDirectory}`)
    log(`  recursive: ${recursive}`)
    log(`  source: ${sourceId}`)
    log(`  server: ${storeParams.host}:${storeParams.port}`)
    log(`  insecure: ${storeParams.insecure}`)

    const peering = await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, true)
    if (!peering) {
        log.err(`cannot connect to store`)
        return
    }

    let store = peering.remoteStore

    let pushResult = await Operations.pushDirectoryToSource(peering, pushedDirectory, sourceId)

    let source = pushResult.directoryDescriptorSha

    await Operations.mergeDirectoryDescriptorToDestination(source, destination, recursive, store)
}

export async function merge(sourceSpec: string, destination: string, recursive: boolean, storeParams: StoreConnectionParams, _verbose: boolean) {
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    let source = await parseSourceSpec(sourceSpec, store)
    if (!source) {
        log.err(`source not specified`)
        return
    }

    await Operations.mergeDirectoryDescriptorToDestination(source, destination, recursive, store)
}

export async function lsDirectoryStructure(storeParams: StoreConnectionParams, directoryDescriptorSha: string, recursive: boolean) {
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    if (directoryDescriptorSha.length != 64) {
        let autocompleted = await store.autoCompleteSha(directoryDescriptorSha)
        if (!autocompleted) {
            log.err(`cannot find autocompletion for ${directoryDescriptorSha}`)
            return
        }
        log(` sha autocompleted: ${directoryDescriptorSha} => ${autocompleted}`)
        directoryDescriptorSha = autocompleted
    }

    let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorSha)

    await showDirectoryDescriptor(directoryDescriptor, store, directoryDescriptorSha.substr(0, 7), recursive)
}

export async function extract(storeParams: StoreConnectionParams, directoryDescriptorSha: string, prefix: string, destinationDirectory: string) {
    let shaCache = new ShaCache.ShaCache('.hb-cache')

    log('connecting to remote store...')
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    if (directoryDescriptorSha.length != 64) {
        let autocompleted = await store.autoCompleteSha(directoryDescriptorSha)
        if (!autocompleted) {
            log.err(`cannot find autocompletion for ${directoryDescriptorSha}`)
            return
        }
        log(` sha autocompleted: ${directoryDescriptorSha} => ${autocompleted}`)
        directoryDescriptorSha = autocompleted
    }

    destinationDirectory = path.resolve(destinationDirectory)

    console.log(`extracting ${directoryDescriptorSha} to ${destinationDirectory}, prefix='${prefix}'...`)
    await extractDirectoryDescriptor(store, shaCache, directoryDescriptorSha, prefix, destinationDirectory)
}

export async function pull(directory: string, sourceId: string, storeParams: StoreConnectionParams, forced: boolean) {
    let remoteStore = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    log(`store ready`)
    log(`transferring`)

    let localStore = new HexaBackupStore(directory)

    let sourceIds = []
    if (sourceId)
        sourceIds.push(sourceId)
    else
        sourceIds = await remoteStore.getSources()

    for (let sourceId of sourceIds)
        await Operations.pullSource(remoteStore, localStore, sourceId, forced)

    log(`pull done`)
}

export async function dbPush(storeParams: StoreConnectionParams, dbParams: DbConnectionParams) {
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

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

export async function dbImage(storeParams: StoreConnectionParams, databaseParams: DbConnectionParams) {
    const sourceId = 'PHOTOS'
    const mimeType = 'image'
    /*const sourceId = 'VIDEOS'
    const mimeType = 'video'*/

    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

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

export async function exifExtract(storeParams: StoreConnectionParams, databaseParams: DbConnectionParams) {
    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    log(`store ready`)

    const client = await DbHelpers.createClient(databaseParams)
    const client2 = await DbHelpers.createClient(databaseParams)

    log(`connected to database`)

    let baseQuery = `from objects o left join object_exifs op on o.sha=op.sha where size > 65635 and mimeType = 'image/jpeg' and (op.sha is null or op.exif::text = '{}'::text)`

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

    log(`processed ${nbRows} images`)

    await cursor.close()

    client.end()
    client2.end()
}

export async function extractSha(storeParams: StoreConnectionParams, sha: string, destinationFile: string) {
    log('connecting to remote store...')

    let shaCache = new ShaCache.ShaCache('.hb-cache')

    let store = (await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, false)).remoteStore

    if (sha.length != 64) {
        let autocompleted = await store.autoCompleteSha(sha)
        if (!autocompleted) {
            log.err(`cannot find autocompletion for ${sha}`)
            return
        }
        log(` sha autocompleted: ${sha} => ${autocompleted}`)
        sha = autocompleted
    }

    destinationFile = path.resolve(destinationFile)
    let size = await store.hasOneShaBytes(sha)

    let fileDesc: Model.FileDescriptor = {
        contentSha: sha,
        isDirectory: false,
        lastWrite: 0,
        name: path.basename(destinationFile),
        size: size
    }
    await extractShaInternal(store, shaCache, fileDesc, path.dirname(destinationFile))

    console.log(`extracted ${sha} to ${destinationFile}, ${Tools.prettySize(size)}...`)
}

async function extractShaInternal(store: IHexaBackupStore, shaCache: ShaCache.ShaCache, fileDesc: Model.FileDescriptor, destinationDirectory: string) {
    console.log(`fetching ${fileDesc.name} ${Tools.prettySize(fileDesc.size)}`)

    let currentReadPosition = 0
    let displayResume = false
    let timer = setInterval(() => {
        displayResume = true
        console.log(` ${Tools.prettySize(currentReadPosition)}`)
    }, 1000)

    try {
        let destinationFilePath = path.join(destinationDirectory, fileDesc.name)

        if (fileDesc.isDirectory) {
            if (!await FsTools.fileExists(destinationFilePath))
                fs.mkdirSync(destinationFilePath)

            if (fileDesc.contentSha) {
                clearInterval(timer)
                timer = null
                await extractDirectoryDescriptor(store, shaCache, fileDesc.contentSha, '', destinationFilePath)
            }
        }
        else {
            let fileLength = await store.hasOneShaBytes(fileDesc.contentSha)

            if (await FsTools.fileExists(destinationFilePath)) {
                let stat = await FsTools.lstat(destinationFilePath)
                currentReadPosition = stat.size
            }

            if (!(await FsTools.fileExists(destinationFilePath)) || fileDesc.contentSha != await shaCache.hashFile(destinationFilePath)) {
                let fd = await FsTools.openFile(destinationFilePath, 'a')

                const maxSize = 1024 * 100
                let writePromise = null
                while (currentReadPosition < fileLength) {
                    let size = fileLength - currentReadPosition
                    if (size > maxSize)
                        size = maxSize

                    let buffer = await store.readShaBytes(fileDesc.contentSha, currentReadPosition, size)

                    if (writePromise) {
                        await writePromise
                        writePromise = null
                    }
                    writePromise = FsTools.writeFileBuffer(fd, currentReadPosition, buffer)

                    currentReadPosition += size
                }

                if (writePromise) {
                    await writePromise
                    writePromise = null
                }

                await FsTools.closeFile(fd)
            }
            else {
                log.dbg(`already extracted ${destinationFilePath}`)
            }

            let contentSha = await HashTools.hashFile(destinationFilePath)
            if (contentSha != fileDesc.contentSha) {
                log.err(`extracted file signature is inconsistent : ${contentSha} != ${fileDesc.contentSha}`)
            }
        }

        if (displayResume)
            console.log(`extracted ${fileDesc.name}`)

        let lastWriteUnix = parseInt((fileDesc.lastWrite / 1000).toFixed(0))
        fs.utimesSync(destinationFilePath, lastWriteUnix, lastWriteUnix)
    }
    catch (err) {
        log.err(`error ${err}`)
    }
    finally {
        clearInterval(timer)
    }
}

async function extractDirectoryDescriptor(store: IHexaBackupStore, shaCache: ShaCache.ShaCache, directoryDescriptorSha: string, prefix: string, destinationDirectory: string) {
    console.log('getting directory descriptor...')
    let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorSha)

    await showDirectoryDescriptorSummary(directoryDescriptor)

    for (let k in directoryDescriptor.files) {
        let fileDesc = directoryDescriptor.files[k]

        if (prefix && !fileDesc.name.startsWith(prefix))
            continue

        await extractShaInternal(store, shaCache, fileDesc, destinationDirectory)
    }
}

export async function push(sourceId: string, pushedDirectory: string, storeParams: StoreConnectionParams, estimateSize: boolean) {
    log('connecting to remote store...')
    log(`push options :`)
    log(`  directory: ${pushedDirectory}`)
    log(`  source: ${sourceId}`)
    log(`  server: ${storeParams.host}:${storeParams.port}`)
    log(`  insecure: ${storeParams.insecure}`)
    log(`  estimateSize: ${estimateSize}`)

    const peering = await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, true)
    if (!peering) {
        log.err(`cannot connect to store`)
        return
    }

    await Operations.pushDirectoryToSource(peering, pushedDirectory, sourceId)
}

export async function pushStore(directory: string, storeParams: StoreConnectionParams, estimateSize: boolean) {
    // TODO : ignore .bak files and shabytes currently received

    log(`push options :`)
    log(`  server: ${storeParams.host}:${storeParams.port}`)
    log(`  insecure: ${storeParams.insecure}`)
    log(`  estimateSize: ${estimateSize}`)

    const peering = await ClientPeering.createClientPeeringFromWebSocket(storeParams.host, storeParams.port, storeParams.token, storeParams.insecure, true)
    if (!peering) {
        log.err(`cannot connect to store`)
        return
    }

    log(`starting push`)

    log(`preparing read store in ${directory}`)
    let pushedDirectory = path.join(directory, '.hb-object')
    log(` store objects directory: ${pushedDirectory}`)

    log(`start push objects`)

    let directoryDescriptorSha = await peering.startPushLoop(pushedDirectory, false)
    log(`store objects pushed (directory descriptor  : ${directoryDescriptorSha})`)

    log(`TODO : push refs`)
}

export async function store(directory: string, port: number, insecure: boolean, databaseParams: DbConnectionParams) {
    return WebServer.runStore(directory, port, insecure, databaseParams)
}

export async function browse(directory: string, verbose: boolean) {
    let queue = new Queue.Queue<Model.FileDescriptor>('filesanddirs')
    let shaCache = new ShaCache.ShaCache('.hb-cache')
    let browser = new DirectoryBrowser.DirectoryBrowser(directory, Queue.waitPusher(queue, 10, 5), shaCache)

    log.setStatus(() => JSON.stringify(browser.stats, null, 4).split('\n'))

    {
        (async () => {
            let popper = Queue.waitPopper(queue)

            while (true) {
                let item = await popper()
                if (!item)
                    break

                let entry = await browser.closeEntry(item.contentSha)

                if (verbose) {
                    if (entry.type == 'directory') {
                        log.dbg(`${entry.descriptorRaw}`)
                    }
                    else {
                        log(`${entry.fullPath}`)
                    }
                }
            }
        })()
    }

    let wholeSha = await browser.start()

    log(`${JSON.stringify(browser.stats, null, 4)}`)
    log(`finished, whole sha is ${wholeSha}`)
}

async function showDirectoryDescriptorSummary(directoryDescriptor: Model.DirectoryDescriptor) {
    let totalSize = 0;
    let nbFiles = 0;
    let nbDirectories = 0;
    directoryDescriptor.files.forEach((fd) => {
        totalSize += fd.size
        if (fd.isDirectory)
            nbDirectories++
        else
            nbFiles++
    });

    console.log(`total ${Tools.prettySize(totalSize)} in ${nbFiles} files, ${nbDirectories} dirs`)
}

async function showDirectoryDescriptor(directoryDescriptor: Model.DirectoryDescriptor, store: IHexaBackupStore, currentPath: string = '.', recursive: boolean = false) {
    console.log(``)
    console.log(`${currentPath}:`)
    showDirectoryDescriptorSummary(directoryDescriptor)

    for (let fd of directoryDescriptor.files) {
        let lastWrite = new Date(fd.lastWrite)

        console.log(`${Tools.displayDate(lastWrite)} ${fd.contentSha ? fd.contentSha.substr(0, 7) : '   -   '} ${Tools.prettySize(fd.size).padStart(12)}   ${fd.name}${fd.isDirectory ? '/' : ''}`)
    }

    if (recursive) {
        for (let fd of directoryDescriptor.files) {
            if (fd.isDirectory && fd.contentSha) {
                let desc = await store.getDirectoryDescriptor(fd.contentSha)
                await showDirectoryDescriptor(desc, store, path.join(currentPath, fd.name), recursive)
            }
        }
    }
}