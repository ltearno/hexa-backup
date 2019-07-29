import { IHexaBackupStore } from './HexaBackupStore'

// TODO if database is configured, use it (if not available, fail)

export function getUserFromRequest(request: any): string {
    return request.headers["x-authenticated-user"] || 'anonymous'
}

export async function getAuthorizedRefsFromHttpRequest(request: any, store: IHexaBackupStore) {
    let user = getUserFromRequest(request)

    return await getAuthorizedRefs(user, store)
}

export async function getAuthorizedRefsFromHttpRequestAsSql(request: any, store: IHexaBackupStore) {
    let tmp = await getAuthorizedRefsFromHttpRequest(request, store)
    if (!tmp || !tmp.length) {
        return null
    }

    return tmp.map(r => `'${r}'`).join(',')
}

export async function getAuthorizedRefs(user: string, store: IHexaBackupStore) {
    let tmp = await getRawAuthorizedRefs(user, store)
    if (!tmp)
        return null

    return tmp.map(r => r.substring('CLIENT_'.length))
}

async function getRawAuthorizedRefs(user: string, store: IHexaBackupStore) {
    try {
        // TODO use ACLs in reference files

        let refs = await store.getRefs()

        switch (user) {
            case 'ltearno':
                return refs

            case 'ayoka':
                return refs.filter(ref => {
                    switch (ref) {
                        case 'CLIENT_MUSIQUE':
                        case 'CLIENT_PHOTOS':
                        case 'CLIENT_VIDEOS':
                            return true
                        default:
                            return false
                    }
                })

            case 'alice.gallas':
                return refs.filter(ref => {
                    switch (ref) {
                        case 'CLIENT_POUR-MAMAN':
                        case 'CLIENT_MUSIQUE':
                            return true
                        default:
                            return false
                    }
                })
        }

        return []
    }
    catch (err) {
        return []
    }
}