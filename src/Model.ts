export interface FileDescriptor {
    name: string;
    isDirectory: boolean;
    size: number,
    lastWrite: number,
    contentSha: string;
}

export interface DirectoryDescriptor {
    files: FileDescriptor[];
}

export interface SourceState {
    currentTransactionId: string;
    currentCommitSha: string;
}

export interface Commit {
    parentSha: string;
    commitDate: number;
    directoryDescriptorSha: string;
}

export interface ShaPoolDescriptor {
    fileName: string;
    sha: string;
    offset: number;
    size: number;
}