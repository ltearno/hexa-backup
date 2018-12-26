export enum RequestType {
    ShaBytes = 1,
    Call = 2,
    HasShaBytes = 3
}

export type HasShaBytes = [RequestType.HasShaBytes, string] // type, sha
export type ShaBytes = [RequestType.ShaBytes, string, number, Buffer] // type, sha, offset, buffer
export type RpcCall = [RequestType.Call, string, ...any[]]
export type RpcQuery = ShaBytes | RpcCall | HasShaBytes
export type RpcReply = any[]