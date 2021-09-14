import * as fs from 'fs'
import * as ClientPeering from './ClientPeering'
import * as Model from './Model'
import { IHexaBackupStore } from './HexaBackupStore'
import { HashTools, LoggerBuilder, NetworkApiNodeImpl, NetworkApi, OrderedJson } from '@ltearno/hexa-js'
import * as PathSpecHelpers from './PathSpecHelpers'
import * as PushDirectory from './PushDirectory'

const KB = 1024
const MB = 1024 * KB
const GB = 1024 * MB
const TB = 1024 * GB

function friendlySize(size: number) {
    if (size > 2 * TB)
        return `${(size / TB).toFixed(1)} TBb`
    if (size > 2 * GB)
        return `${(size / GB).toFixed(1)} Gb`
    if (size > 2 * MB)
        return `${(size / MB).toFixed(1)} Mb`
    if (size > 2 * KB)
        return `${(size / KB).toFixed(1)} kb`
    if (size > 1)
        return `${size} bytes`
    if (size == 1)
        return `1 byte`
    return `empty`
}

const log = LoggerBuilder.buildLogger('Operations')

export function connectToRemoteSocket(host: string, port: number, token: string, headers: { [name: string]: string }, insecure: boolean): Promise<NetworkApi.WebSocket> {
    return new Promise((resolve, reject) => {
        let network = new NetworkApiNodeImpl.NetworkApiNodeImpl()
        let url = `${insecure ? 'ws' : 'wss'}://${host}:${port}/hexa-backup`
        log(`connecting to ${url}`)
        headers = headers ? JSON.parse(JSON.stringify(headers)) : {}
        if (token)
            headers["Authorization"] = `Bearer ${token}`
        let ws = network.createClientWebSocket(url, headers)
        let opened = false

        ws.on('open', () => {
            opened = true
            resolve(ws)
        })

        ws.on('error', err => {
            log.err(`websocket error: ${err}`)
            if (!opened)
                resolve(null)
            else
                ws.close()
        })
    })
}

export interface InMemoryDirectoryDescriptor {
    files: InMemoryFileDescriptor[]
}

export interface InMemoryFileDescriptor {
    name: string
    isDirectory: boolean
    size: number
    lastWrite: number
    content: string | InMemoryDirectoryDescriptor
}

export async function mergeDirectoryDescriptors(source: Model.DirectoryDescriptor, merged: Model.DirectoryDescriptor): Promise<Model.DirectoryDescriptor> {
    let names = new Set<string>()

    let result = { files: [] } as Model.DirectoryDescriptor

    for (let sourceDescriptor of [merged, source]) {
        for (let entry of sourceDescriptor.files) {
            if (!names.has(entry.name)) {
                result.files.push(entry)
                names.add(entry.name)
            }
        }
    }

    return JSON.parse(JSON.stringify(result))
}

export function createInMemoryDirectoryDescriptor(desc: Model.DirectoryDescriptor): InMemoryDirectoryDescriptor {
    return {
        files: !desc.files ? null : desc.files.map(item => createInMemoryFileDescriptor(item))
    }
}

export function createInMemoryFileDescriptor(descriptor: Model.FileDescriptor): InMemoryFileDescriptor {
    return {
        name: descriptor.name,
        size: descriptor.size,
        lastWrite: descriptor.lastWrite,
        isDirectory: descriptor.isDirectory,
        content: descriptor.contentSha
    }
}

export async function resolve(item: InMemoryFileDescriptor, store: IHexaBackupStore) {
    if (typeof item.content === 'string') {
        item.content = createInMemoryDirectoryDescriptor(await store.getDirectoryDescriptor(item.content))
    }
}

export async function pushDirectoryToSource(peering: ClientPeering.Peering, pushedDirectory: string, sourceId: string) {
    try {
        let store = peering.remoteStore

        log(`starting push`)

        const pushDirectory = new PushDirectory.PushDirectory()
        let directoryDescriptorSha = await pushDirectory.startPushLoop(pushedDirectory, true, peering.remoteStore)
        log(`directory descriptor  : ${directoryDescriptorSha}`)

        let commitSha = await store.registerNewCommit(sourceId, directoryDescriptorSha)

        log(`finished push, commit : ${commitSha}`)

        return {
            directoryDescriptorSha,
            commitSha
        }
    }
    catch (err) {
        log.err(`error pushing directory to source ${err}`)
        return null
    }
}

export async function getSourceCurrentDirectoryDescriptor(sourceId: string, store: IHexaBackupStore) {
    let sourceState = await store.getSourceState(sourceId)
    if (!sourceState || !sourceState.currentCommitSha)
        return null

    let commit = await store.getCommit(sourceState.currentCommitSha)
    if (!commit || !commit.directoryDescriptorSha)
        return null

    let descriptor = await store.getDirectoryDescriptor(commit.directoryDescriptorSha)
    return descriptor
}

export async function pushLocalFileToStore(filePath: string, store: IHexaBackupStore): Promise<{ sha: string; path: string; size: number; }> {
    let stats = fs.statSync(filePath)
    let contentSha = await HashTools.hashFile(filePath)
    let offset = await store.hasOneShaBytes(contentSha)

    const fd = fs.openSync(filePath, 'r')
    if (!fd) {
        log.err(`error reading`)
        fs.closeSync(fd)
        return null
    }

    while (offset < stats.size) {
        const length = Math.min(4 * 1024 * 1024, stats.size - offset)
        let buffer = Buffer.alloc(length)

        let nbRead = fs.readSync(fd, buffer, 0, length, offset)
        if (nbRead != length) {
            log.err(`inconsistent read`)
            break
        }

        await store.putShaBytes(contentSha, offset, buffer)

        offset += length
    }

    fs.closeSync(fd)

    let validated = await store.validateShaBytes(contentSha)
    if (validated) {
        return {
            sha: contentSha,
            path: filePath,
            size: stats.size
        }
    }
    else {
        log.err(`cannot validate downloaded sha`)
        return null
    }
}

export async function mergeDirectoryDescriptorToDestination(source: string, destination: string, recursive: boolean, store: IHexaBackupStore) {
    let parsedDestination = PathSpecHelpers.parseTargetSpec(destination)
    if (!parsedDestination) {
        log.err(`destination not specified`)
        return
    }

    log(`source : ${source}`)
    log(`destination : ${parsedDestination.sourceId} @ ${parsedDestination.pathParts.join('/')}`)

    let destinationSourceState = await store.getSourceState(parsedDestination.sourceId)
    if (!destinationSourceState) {
        log(`destination source state '${parsedDestination.sourceId}' does not exist yet`)
    }
    
    if (!destinationSourceState || !destinationSourceState.currentCommitSha) {
        log(`destination '${parsedDestination.sourceId}' has no commit specified, creating and registering a new initial empty commit...`)

        const createdEmptyDirectoryDescriptorSha = await pushDirectoryDescriptor({ files: [] }, store)
        if (!createdEmptyDirectoryDescriptorSha) {
            log.err(`cannot create and push an empty directory descriptor to initiate the destination source`)
            return
        }

        let createdCommitSha = await store.registerNewCommit(parsedDestination.sourceId, createdEmptyDirectoryDescriptorSha)
        if (!createdCommitSha) {
            log.err(`did not manage to register a first commit on the source, aborting`)
            return
        }
        else {
            log(`created empty commit ${createdCommitSha}`)
        }

        destinationSourceState = await store.getSourceState(parsedDestination.sourceId)
        if (!destinationSourceState) {
            log.err(`destination source state not found after creating initial commit`)
            return
        }
    }

    let commit = await store.getCommit(destinationSourceState.currentCommitSha)
    if (!commit) {
        log.err(`destination's current commit ${destinationSourceState.currentCommitSha} not found`)
        return
    }

    if (!commit.directoryDescriptorSha) {
        log.err(`current commit has no root directory descriptor`)
        return
    }

    let currentRootDirectoryDescriptor = await store.getDirectoryDescriptor(commit.directoryDescriptorSha)
    if (!currentRootDirectoryDescriptor) {
        log.err(`cannot fetch root directory descriptor ${commit.directoryDescriptorSha}`)
        return
    }

    let rootInMemoryDirectoryDescriptor = createInMemoryDirectoryDescriptor(currentRootDirectoryDescriptor)

    let targetInMemoryDirectoryDescriptor = rootInMemoryDirectoryDescriptor
    for (let subDirName of parsedDestination.pathParts) {
        let subDirItem = targetInMemoryDirectoryDescriptor.files.find(item => item.name == subDirName)
        if (!subDirItem) {
            log.wrn(`creating sub directory '${subDirName}' in destination path`)
            subDirItem = {
                name: subDirName,
                content: { files: [] },
                isDirectory: true,
                size: 0,
                lastWrite: Date.now()
            }
            targetInMemoryDirectoryDescriptor.files.push(subDirItem)
        }
        else {
            await resolve(subDirItem, store)
        }

        if (typeof subDirItem.content == 'string') {
            log.err(`cannot resolve in memory directory descriptor with content ${subDirItem.content}`)
            return
        }

        targetInMemoryDirectoryDescriptor = subDirItem.content
    }

    if (!targetInMemoryDirectoryDescriptor) {
        log.err(`big error h36@Sg2887, bye`)
        return
    }

    let mergedDescriptor = await store.getDirectoryDescriptor(source)
    if (!mergedDescriptor) {
        log.err(`cannot load source descriptor ${source}`)
        return
    }

    let sourceInMemoryDirectoryDescriptor = createInMemoryDirectoryDescriptor(mergedDescriptor)

    await recMerge(sourceInMemoryDirectoryDescriptor, targetInMemoryDirectoryDescriptor, recursive, store)

    let newRootDescriptorSha = await saveInMemoryDirectoryDescriptor(rootInMemoryDirectoryDescriptor, store)
    if (!newRootDescriptorSha) {
        log.err(`failed to save new root descriptor`)
        return
    }

    log(`new root directory descriptor : ${newRootDescriptorSha}`)

    let commitSha = await store.registerNewCommit(parsedDestination.sourceId, newRootDescriptorSha)

    log(`validated commit ${commitSha} on source ${parsedDestination.sourceId}`)
}

export async function commitDirectoryDescriptor(sourceId: string, descriptor: Model.DirectoryDescriptor, store: IHexaBackupStore): Promise<string> {
    let descriptorSha = await pushDirectoryDescriptor(descriptor, store)
    if (!descriptorSha)
        return null

    let commitSha = await store.registerNewCommit(sourceId, descriptorSha)
    return commitSha
}

export async function pushDirectoryDescriptor(descriptor: Model.DirectoryDescriptor, store: IHexaBackupStore): Promise<string> {
    let stringified = OrderedJson.stringify(descriptor)

    let content = Buffer.from(stringified, 'utf8')
    let sha = await HashTools.hashString(stringified)

    let len = await store.hasOneShaBytes(sha)
    if (len != content.length) {
        log(`send directory ${sha}`)
        await store.putShaBytes(sha, 0, content)
        let ok = await store.validateShaBytes(sha)
        if (!ok) {
            log.err(`sha not validated ${sha}`)
            return null
        }
    }
    else {
        log.dbg(`directory already in store ${sha}`)
    }

    return sha
}

async function recMerge(src: InMemoryDirectoryDescriptor, dst: InMemoryDirectoryDescriptor, recursive: boolean, store: IHexaBackupStore) {
    let namesIndex = new Map<string, number>()
    dst.files.forEach((item, index) => namesIndex.set(item.name, index))

    for (let item of src.files) {
        if (item.isDirectory && !recursive) {
            log(`skipped dir ${item.name} (non recursive)`)
            continue
        }

        if (namesIndex.has(item.name)) {
            log(`existing file/dir ${item.name}`)
            let existing = dst.files[namesIndex.get(item.name)]

            if (item.content == existing.content) {
                log(`same content, skipping`)
                continue
            }

            if (item.isDirectory != existing.isDirectory) {
                log.err(`not same types, skipping ! src:${item.isDirectory} dst:${item.isDirectory}`)
                continue
            }

            if (item.name != existing.name) {
                log.err(`BIG ERROR 938763987692 not same name, skipping ! src:${item.name} dst:${item.name}`)
                return
            }

            if (item.isDirectory) {
                await resolve(item, store)
                await resolve(existing, store)
                await recMerge(item.content as InMemoryDirectoryDescriptor, existing.content as InMemoryDirectoryDescriptor, recursive, store)
            }
            else {
                log(`replacing file ${item.name} with sha ${existing.content} by sha ${item.content}`)
                existing.content = item.content
                existing.lastWrite = item.lastWrite
                existing.size = item.size
            }
        }
        else {
            log(`added ${item.name}`)
            dst.files.push(item)
        }
    }
}

// returns the saved directory descriptr's sha
async function saveInMemoryDirectoryDescriptor(inMemoryDescriptor: InMemoryDirectoryDescriptor, store: IHexaBackupStore): Promise<string> {
    for (let item of inMemoryDescriptor.files) {
        if (typeof item.content != 'string') {
            item.content = await saveInMemoryDirectoryDescriptor(item.content, store)
        }
    }

    // now all sub items have sha, it's time to convert and push descriptor
    let converted: Model.DirectoryDescriptor = {
        files: inMemoryDescriptor.files.map(item => {
            return {
                name: item.name,
                isDirectory: item.isDirectory,
                lastWrite: item.lastWrite,
                size: item.size,
                contentSha: item.content as string
            }
        })
    }

    return await pushDirectoryDescriptor(converted, store)
}


/** pulling */

async function pullFile(sourceStore: IHexaBackupStore, destinationStore: IHexaBackupStore, sha: string, comment: string) {
    if (await destinationStore.validateShaBytes(sha)) {
        log.dbg(`already have sha valid on destination`)
        return true
    }

    let sourceLength = await sourceStore.hasOneShaBytes(sha)
    let targetLength = await destinationStore.hasOneShaBytes(sha)
    if (sourceLength == targetLength) {
        log.dbg(`already have sha ${sha}`)
        return true
    }

    if (sourceLength < targetLength) {
        log.err(`error, transferring something smaller than what we have here ${sha}`)
        return false
    }

    log(`transferring sha ${sha}, ${targetLength ? `${friendlySize(sourceLength - targetLength)} of ` : ''}${friendlySize(sourceLength)} ${comment}`)

    let showSuccess = false
    let lastSpeedDisplay = Date.now()
    let lastSentBytesAmount = 0
    let lastSentBytesTime = 0
    let offset = targetLength
    let lastPromise = null
    while (offset < sourceLength) {
        let speed = lastSentBytesTime != 0 ? (1000 * lastSentBytesAmount) / lastSentBytesTime : 0

        lastSentBytesTime = Date.now()

        let len = Math.min(1024 * 1024 * 5, sourceLength - offset)

        if (offset && ((Date.now() - lastSpeedDisplay) > 1000 * 10)) {
            log(`transfer ${friendlySize(offset)}/${friendlySize(sourceLength)} (${Math.floor(100 * offset / sourceLength).toFixed(2)}%, ${friendlySize(speed)}/s)...`)
            lastSpeedDisplay = Date.now()
            showSuccess = true
        }

        let buffer = await sourceStore.readShaBytes(sha, offset, len)
        if (lastPromise) {
            await lastPromise
            lastPromise = null
        }
        lastPromise = destinationStore.putShaBytes(sha, offset, buffer)
        //await lastPromise
        //lastPromise = null

        lastSentBytesTime = Date.now() - lastSentBytesTime
        lastSentBytesAmount = len

        offset += len
    }

    await lastPromise

    let ok = await destinationStore.validateShaBytes(sha)
    if (ok) {
        if (showSuccess)
            log(`transferred successfully sha ${sha}`)
        return true
    }
    else {
        log.err(`error transferring sha ${sha}`)
        return false
    }
}

async function pullDirectoryDescriptor(sourceStore: IHexaBackupStore, destinationStore: IHexaBackupStore, directoryDescriptorSha: string, comment: string) {
    if (await destinationStore.validateShaBytes(directoryDescriptorSha)) {
        log.dbg(`already have descriptor valid on destination`)
        return true
    }

    let sourceLength = await sourceStore.hasOneShaBytes(directoryDescriptorSha)
    let targetLength = await destinationStore.hasOneShaBytes(directoryDescriptorSha)
    if (sourceLength == targetLength) {
        log.dbg(`already have directoryDescriptor`)
        return true
    }

    log(`pulling directory descriptor ${directoryDescriptorSha} [${comment}] (${friendlySize(sourceLength - targetLength)})`)

    let directoryDescriptorBuffer = await sourceStore.readShaBytes(directoryDescriptorSha, 0, sourceLength)
    if (!directoryDescriptorBuffer) {
        log.err(`cannot fetch directoryDescriptor from source store ${directoryDescriptorSha}`)
        return false
    }
    let directoryDescriptor = (() => {
        try {
            return JSON.parse(directoryDescriptorBuffer.toString('utf-8')) as Model.DirectoryDescriptor
        }
        catch (err) {
            return null
        }
    })()
    if (!directoryDescriptor) {
        log.err(`cannot parse directoryDescriptor ${directoryDescriptorSha}`)
        return false
    }
    //let directoryDescriptor = await sourceStore.getDirectoryDescriptor(directoryDescriptorSha)

    log(`${directoryDescriptor.files.length} files in directory descriptor`)

    let countNullContentSha = 0

    for (let fileIdx in directoryDescriptor.files) {
        const file = directoryDescriptor.files[fileIdx]

        // skip badly formatted entries (they exist....)
        if (!file.contentSha) {
            log.dbg(`entry's contentSha is null in directory descriptor for ${file.name}`)
            countNullContentSha++
            continue
        }

        if (file.isDirectory) {
            let ok = await pullDirectoryDescriptor(sourceStore, destinationStore, file.contentSha, `${comment}/${file.name}`)
            if (!ok)
                return false
        }
        else {
            let ok = await pullFile(sourceStore, destinationStore, file.contentSha, `${parseInt(fileIdx) + 1}/${directoryDescriptor.files.length} ${file.name}`)
            if (!ok)
                return false
        }
    }

    await destinationStore.putShaBytes(directoryDescriptorSha, 0, directoryDescriptorBuffer)
    let ok = await destinationStore.validateShaBytes(directoryDescriptorSha)
    if (ok) {
        log(`ok, synced directory descriptor ${directoryDescriptorSha} ${countNullContentSha ? `(${countNullContentSha} null contentSha entries)` : ''}`)
        return true
    }
    else {
        log.err(`failed to validate synced directory descriptor!`)
        return false
    }
}

export async function pullSource(sourceStore: IHexaBackupStore, destinationStore: IHexaBackupStore, sourceId: string, forced: boolean) {
    log(`pulling source ${sourceId}`)

    let sourceState = await sourceStore.getSourceState(sourceId)

    let currentCommitSha = sourceState.currentCommitSha

    let destinationState = await destinationStore.getSourceState(sourceId)
    log.dbg(`     source state : ${JSON.stringify(sourceState)}`)
    log.dbg(`destination state : ${JSON.stringify(destinationState)}`)
    if (!destinationState) {
        log.wrn(`destination ${sourceId} does not exist, will be created`)
    }
    if (destinationState && destinationState.readOnly) {
        log.wrn(`destination state is read-only, we fetch data but won't commit`)
    }
    if (destinationState && destinationState.currentCommitSha) {
        // avoid conflicts !
        let accepted = false
        let browsedCommitSha = currentCommitSha
        while (browsedCommitSha) {
            log.dbg(`browse source commit ${browsedCommitSha}`)
            if (browsedCommitSha == destinationState.currentCommitSha) {
                log.dbg(`ok, tip of the destination, accepted`)
                accepted = true
                break
            }

            let browsedCommit = await sourceStore.getCommit(browsedCommitSha)
            if (!browsedCommit) {
                log.wrn(`pull with source conflict !`)
                break
            }
            browsedCommitSha = browsedCommit.parentSha
        }

        if (!accepted) {
            if (!forced) {
                log.err(`cannot pull since pull conflicts ! use --force to force`)
                return false
            }
            else {
                log.wrn(`conflict ignored because force option is on`)
            }
        }
    }

    let errors = []

    let depth = 0

    while (currentCommitSha) {
        let sourceLength = await sourceStore.hasOneShaBytes(currentCommitSha)
        let targetLength = await destinationStore.hasOneShaBytes(currentCommitSha)
        if (sourceLength == targetLength) {
            log.dbg(`already have commit ${currentCommitSha}`)
            break
        }

        log(`pulling commit on source ${sourceId}, depth ${depth}, sha ${currentCommitSha} (${friendlySize(sourceLength - targetLength)})`)

        let commit = await sourceStore.getCommit(currentCommitSha)

        let ok = await pullDirectoryDescriptor(sourceStore, destinationStore, commit.directoryDescriptorSha, `(${sourceId})`)
        if (!ok) {
            let err = `error pulling directory descriptor ${commit.directoryDescriptorSha}`
            errors.push(err)
            log.err(err)
        }
        else {
            // copy commit
            ok = await pullFile(sourceStore, destinationStore, currentCommitSha, `commit in source ${sourceId}`)
            if (!ok) {
                let err = `failed to copy commit ${currentCommitSha}`
                errors.push(err)
                log.err(err)
            }
        }

        currentCommitSha = commit.parentSha
        depth++
    }

    if (errors.length) {
        log.err(`encountered errors while pulling source ${sourceId}:`)
        errors.forEach(err => log.err(err))
        return false
    }

    if (destinationState && destinationState.readOnly) {
        log.wrn(`since destination is read only, we don't update its commit, but all data has been fetched`)
    }
    else {
        log.dbg(`      update destination source's commit to ${sourceState.currentCommitSha}`)
        if (!destinationState) {
            destinationState = {
                currentCommitSha: sourceState.currentCommitSha,
                readOnly: false
            }
        }
        else {
            destinationState.currentCommitSha = sourceState.currentCommitSha
        }
        // THIS IS HACKY BECAUSE HERE WE COULD CHANGE ANY FIELD OF THE SOURCE.
        // WE SHOULD HAVE A REMOTE CALL TO JUST ASK TO UPDATE THE COMMIT_SHA OF A SOURCE, AND THE SERVER DOES WHAT NEEDS TO BE DONE
        await destinationStore.setClientState(sourceId, destinationState)
    }

    return true
}