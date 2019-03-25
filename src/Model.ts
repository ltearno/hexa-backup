export interface FileDescriptor {
    name: string
    isDirectory: boolean
    size: number
    lastWrite: number
    contentSha: string
}

export interface DirectoryDescriptor {
    files: FileDescriptor[]
}

export interface SourceState {
    currentCommitSha: string
}

export interface Commit {
    parentSha: string
    commitDate: number
    directoryDescriptorSha: string
}