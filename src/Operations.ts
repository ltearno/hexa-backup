import * as Model from './Model'

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

export async function addOrUpdateDescriptor(directoryDescriptor: Model.DirectoryDescriptor, itemDescriptor: Model.FileDescriptor) {
    directoryDescriptor = JSON.parse(JSON.stringify(directoryDescriptor))
    directoryDescriptor.files = directoryDescriptor.files.filter(item => item.name != itemDescriptor.name)
    directoryDescriptor.files.push(itemDescriptor)
}