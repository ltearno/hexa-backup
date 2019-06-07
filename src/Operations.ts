import * as ClientPeering from './ClientPeering'
import * as Model from './Model'
import { IHexaBackupStore } from './HexaBackupStore'
import { LoggerBuilder, OrderedJson, HashTools } from '@ltearno/hexa-js'
import * as PathSpecHelpers from './PathSpecHelpers'

const log = LoggerBuilder.buildLogger('Operations')
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
    let store = peering.remoteStore

    log(`starting push`)

    let directoryDescriptorSha = await peering.startPushLoop(pushedDirectory, true)
    log(`directory descriptor  : ${directoryDescriptorSha}`)

    let commitSha = await store.registerNewCommit(sourceId, directoryDescriptorSha)

    log(`finished push, commit : ${commitSha}`)

    return {
        directoryDescriptorSha,
        commitSha
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
        log.err(`destination source state not found`)
        return
    }

    if (!destinationSourceState.currentCommitSha) {
        log.err(`destination has no commit specified`)
        return
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

export async function pushDirectoryDescriptor(descriptor: Model.DirectoryDescriptor, store: IHexaBackupStore): Promise<string> {
    let stringified = OrderedJson.stringify(descriptor)

    let content = Buffer.from(stringified, 'utf8')
    let sha = HashTools.hashStringSync(stringified)

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
