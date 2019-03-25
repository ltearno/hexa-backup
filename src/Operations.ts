import * as Model from './Model'

export async function mergeDirectoryDescriptors(source: Model.DirectoryDescriptor, merged: Model.DirectoryDescriptor): Promise<Model.DirectoryDescriptor> {
    let namesInSource = new Set<string>()
    source.files.forEach(file => namesInSource.add(file.name))

    let result = JSON.parse(JSON.stringify(source)) as Model.DirectoryDescriptor
    for (let entry of merged.files) {
        if (!namesInSource.has(entry.name))
            result.files.push(entry)
    }

    return result
}