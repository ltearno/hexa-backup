import * as Model from './Model'
import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
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