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





export interface ClientState {
    currentTransactionId: string;
    currentTransactionContent: { [key: string]: FileDescriptor };
    currentCommitSha: string;
}

export interface Commit {
    parentSha: string;
    commitDate: number;
    directoryDescriptorSha: string;
}