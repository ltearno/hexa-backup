export interface FileInfo {
    name: string;
    isDirectory: boolean;
    lastWrite: number;
    size: number;
}

export interface FileAndShaInfo extends FileInfo {
    contentSha: string;
}

export const MSG_TYPE_ASK_SHA_STATUS = 0
export const MSG_TYPE_REP_SHA_STATUS = 1
export const MSG_TYPE_ADD_SHA_IN_TX = 2
export const MSG_TYPE_SHA_BYTES = 3
export const MSG_TYPE_SHA_BYTES_COMMIT = 4
export const MSG_TYPE_ASK_BEGIN_TX = 5 // (clientId)
export const MSG_TYPE_REP_BEGIN_TX = 6 // (txId)
export const MSG_TYPE_COMMIT_TX = 7