import * as express from 'express'
import * as bodyParser from 'body-parser'
import * as ShaCache from './ShaCache'
import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
import { HashTools, FsTools, LoggerBuilder, Queue, ExpressTools, Transport, NetworkApiNodeImpl, NetworkApi, OrderedJson } from '@ltearno/hexa-js'
import * as Model from './Model'
import * as fs from 'fs'
import * as http from 'http'
import * as https from 'https'
import * as path from 'path'
import * as DirectoryBrowser from './DirectoryBrowser'
import {
    RequestType,
    RpcQuery,
    RpcReply
} from './RPC'
import * as ClientPeering from './ClientPeering'
import * as Tools from './Tools'
import { resolve } from 'url';

const log = LoggerBuilder.buildLogger('Commands')


interface TreeDirectoryInfo {
    files: Model.FileDescriptor[]
    name: string
    lastWrite: number
    directories: TreeDirectoryInfo[]
}

function connectToRemoteSocket(host: string, port: number, insecure: boolean): Promise<NetworkApi.WebSocket> {
    return new Promise((resolve, reject) => {
        let network = new NetworkApiNodeImpl.NetworkApiNodeImpl()
        let ws = network.createClientWebSocket(`${insecure ? 'ws' : 'wss'}://${host}:${port}/hexa-backup`)
        let opened = false

        ws.on('open', () => {
            opened = true
            resolve(ws)
        })

        ws.on('error', err => {
            if (!opened)
                reject(err)
            else {
                log.err(`websocket error: ${err}`)
                ws.close()
            }
        })
    })
}





export async function refs(storeIp, storePort, verbose, insecure: boolean) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    let peering = new ClientPeering.Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

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


export async function sources(storeIp, storePort, verbose, insecure: boolean) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    let peering = new ClientPeering.Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

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

export async function stats(storeIp, storePort, verbose, insecure: boolean) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    let peering = new ClientPeering.Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    console.log(`stats on store`)

    let stats = await store.stats()

    if (stats == null) {
        console.log()
        console.log(`stats not found !`)
        return
    }

    log(JSON.stringify(stats, null, 4))
}


export async function history(sourceId: string, storeIp: string, storePort: number, verbose: boolean, insecure: boolean) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    let peering = new ClientPeering.Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

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

async function loadTreeDirectoryInfoFromDirectoryDescriptor(store: IHexaBackupStore, directoryDescriptor: Model.DirectoryDescriptor): Promise<TreeDirectoryInfo> {
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

export async function normalize(sourceId: string, storeIp: string, storePort: number, verbose: boolean, insecure: boolean) {
    log(`connecting to remote store ${storeIp}:${storePort}...`)

    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    let peering = new ClientPeering.Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

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

export async function lsDirectoryStructure(storeIp: string, storePort: number, directoryDescriptorSha: string, recursive: boolean, insecure: boolean) {
    log('connecting to remote store...')

    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    let peering = new ClientPeering.Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

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

export async function extract(storeIp: string, storePort: number, directoryDescriptorSha: string, prefix: string, destinationDirectory: string, insecure: boolean) {
    log('connecting to remote store...')

    let shaCache = new ShaCache.ShaCache('.hb-cache')

    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    let peering = new ClientPeering.Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

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

export async function extractSha(storeIp: string, storePort: number, sha: string, destinationFile: string, insecure: boolean) {
    log('connecting to remote store...')

    let shaCache = new ShaCache.ShaCache('.hb-cache')

    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    let peering = new ClientPeering.Peering(ws, false)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

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

export async function push(sourceId: string, pushedDirectory: string, storeIp: string, storePort: number, estimateSize: boolean, insecure: boolean) {
    log('connecting to remote store...')
    log(`push options :`)
    log(`  directory: ${pushedDirectory}`)
    log(`  source: ${sourceId}`)
    log(`  server: ${storeIp}:${storePort}`)
    log(`  estimateSize: ${estimateSize}`)
    log(`  insecure: ${insecure}`)

    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    let peering = new ClientPeering.Peering(ws, true)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    log(`starting push`)

    let directoryDescriptorSha = await peering.startPushLoop(pushedDirectory, true)
    log(`directory descriptor  : ${directoryDescriptorSha}`)

    let commitSha = await store.registerNewCommit(sourceId, directoryDescriptorSha)

    log(`finished push, commit : ${commitSha}`)
}

export async function pushStore(directory: string, storeIp: string, storePort: number, estimateSize: boolean, insecure: boolean) {
    // TODO : ignore .bak files and shabytes currently received

    log(`push options :`)
    log(`  server: ${storeIp}:${storePort}`)
    log(`  estimateSize: ${estimateSize}`)

    log('connecting to remote store...')
    let ws = await connectToRemoteSocket(storeIp, storePort, insecure)
    log('connected')

    log(`preparing read store in ${directory}`)
    //let localStore = new HexaBackupStore(directory)
    //let pushedDirectory = localStore.getObjectRepository().getRootPath()
    let pushedDirectory = path.join(directory, '.hb-object')
    log(` store objects directory: ${pushedDirectory}`)

    log(`start push objects`)
    let peering = new ClientPeering.Peering(ws, true)
    peering.start().then(_ => log(`finished peering`))

    let store = peering.remoteStore

    log(`starting push`)

    let directoryDescriptorSha = await peering.startPushLoop(pushedDirectory, false)
    log(`store objects pushed (directory descriptor  : ${directoryDescriptorSha})`)

    log(`TODO : push refs`)
}

export async function store(directory: string, port: number, insecure: boolean) {
    console.log(`preparing store in ${directory}`)
    let store = new HexaBackupStore(directory)

    console.log('server initialisation')

    let app: any = express()
    app.use(bodyParser.json())

    let server: any = null

    if (insecure) {
        //app = ExpressTools.createExpressApp(port)
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
    console.log(`listening ${insecure ? 'HTTP' : 'HTTPS'} on ${port}`)

    require('express-ws')(app, server)

    console.log(`base dir: ${path.dirname(__dirname)}`)
    app.use('/public', express.static(path.join(path.dirname(__dirname), 'static')))

    app.get('/refs', async (req, res) => {
        let refs = await store.getRefs()
        res.send(JSON.stringify(refs))
    });

    app.get('/refs/:id', async (req, res) => {
        let id = req.params.id

        let result = await store.getSourceState(id)

        res.send(JSON.stringify(result))
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
                const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1
                const chunksize = (end - start) + 1
                const head = {
                    'Content-Range': `bytes ${start}-${end}/${fileSize}`,
                    'Accept-Ranges': 'bytes',
                    'Content-Length': chunksize,
                    'Content-Type': req.query.type,
                }

                if (req.query.fileName)
                    head['Content-Disposition'] = `attachment; filename="${req.query.fileName}"`

                res.writeHead(206, head)
                store.readShaAsStream(sha, start, end).pipe(res)
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
            res.send(`{"error":"missing sha ${sha}, ${err}!"}`)
        }
    });

    let thumbnailCache = new Map<string, Buffer>()
    let thumbnailCacheEntries = []

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
            res.send(`{"error":"missing sha ${sha}!"}`)
        }
    });

    let mediumCache = new Map<string, Buffer>()
    let mediumCacheEntries = []

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
    });

    const createSmallVideoNotReady = (sha: string): Promise<string> => {
        return new Promise(resolve => {
            const ffmpeg = require('fluent-ffmpeg')

            let destFile = `/tmp/svhb-${sha}.mp4`
            if (fs.existsSync(destFile))
                return destFile

            let rawStream = store.readShaAsStream(sha, 0, -1)
            ffmpeg(rawStream)
                .videoCodec('libx264')
                .audioCodec('libmp3lame')
                .size('320x240')
                .on('error', err => {
                    console.error('ffmpeg error occurred: ' + err.message)
                    resolve(null)
                })
                .on('end', () => {
                    console.log(`finished video conversion to ${destFile}`)
                    resolve(destFile)
                })
                .save(destFile)
        })
    }

    const createSmallVideo = (sha: string): Promise<string> => {
        return new Promise(resolve => {
            let destFile = `/tmp/svhb-${sha}.mp4`
            if (fs.existsSync(destFile))
                resolve(destFile)

            let inputFile = store.getShaFileName(sha)
            //let rawStream = store.readShaAsStream(sha, 0, -1)

            const { spawn } = require('child_process')
            const child = spawn('ffmpeg', [
                '-i',
                inputFile,///tmp/svhb-636632f471fddfaafc410ad608ddda1b964780caccebed6d34eea8e41cec7fc9.mp4
                '-vf',
                'scale=w=320:h=240',
                destFile
            ])

            //rawStream.pipe(child.stdin)

            child.stdout.on('data', (data) => {
                console.log(`child stdout:\n${data}`);
            });

            child.stderr.on('data', (data) => {
                console.error(`child stderr:\n${data}`);
            });

            child.on('exit', (code, signal) => {
                if (!code)
                    resolve(destFile)
                else
                    resolve(null)
            })
        })
    }

    app.get('/sha/:sha/plugins/video/small', async (req, res) => {
        let sha = req.params.sha
        if (sha == null || sha == 'null') {
            res.send(`{"error":"input validation (sha is ${sha})"}`)
            return
        }

        try {
            res.set('Content-Type', 'video/mp4')

            let fileName = await createSmallVideo(sha)
            if (!fileName) {
                res.send(`{"error":"converting video content (sha is ${sha})"}`)
                return
            }

            // ffmpeg -i /home/arnaud/repos/persos/hexa-backup/.hb-object/63/636632f471fddfaafc410ad608ddda1b964780caccebed6d34eea8e41cec7fc9 -vf scale=w=320:h=240 test.mp4
            const range = req.headers.range

            let stat = fs.statSync(fileName)
            const fileSize = stat.size

            if (range) {
                const parts = range.replace(/bytes=/, "").split("-")
                const start = parseInt(parts[0])
                const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1
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
            res.send(`{"error":"missing sha ${sha}!"}`)
        }
    });

    app.ws('/hexa-backup', async (ws: NetworkApi.WebSocket, req: any) => {
        console.log(`serving new client ws`)

        let rpcTxIn = new Queue.Queue<RpcQuery>('rpc-tx-in')
        let rpcTxOut = new Queue.Queue<{ request: RpcQuery; reply: RpcReply }>('rpc-tx-out')
        let rpcRxOut = new Queue.Queue<{ id: string; request: RpcQuery }>('rpc-rx-out')
        let rpcRxIn = new Queue.Queue<{ id: string; reply: RpcReply }>('rpc-rx-in')

        let transport = new Transport.Transport(Queue.waitPopper(rpcTxIn), Queue.directPusher(rpcTxOut), Queue.directPusher(rpcRxOut), Queue.waitPopper(rpcRxIn), ws)
        transport.start()

        ws.on('error', err => {
            console.log(`error on ws ${err}`)
            ws.close()
        })

        ws.on('close', () => {
            console.log(`closed ws`)
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
                            console.log(`not found method ${methodName} in store !`)
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

        console.log(`bye bye client ws !`)
    })

    console.log(`ready on port ${port} !`);
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