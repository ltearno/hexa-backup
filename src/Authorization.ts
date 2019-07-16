import { IHexaBackupStore } from './HexaBackupStore'

export async function getAuthorizedRefsFromHttpRequest(request: any, response: any, store: IHexaBackupStore) {
    let user = request.headers["x-authenticated-user"] || 'anonymous'
    let tmp = await getAuthorizedRefs(user, store)
    if (!tmp || !tmp.length) {
        return null
    }

    return tmp.join(',')
}

export async function getAuthorizedRefs(user: string, store: IHexaBackupStore) {
    let tmp = await getRawAuthorizedRefs(user, store)
    if (!tmp)
        return null

    return tmp.map(r => `'${r.substring('CLIENT_'.length)}'`)
}

async function getRawAuthorizedRefs(user: string, store: IHexaBackupStore) {
    try {
        let refs = await store.getRefs()

        // this is highly a hack, will be moved elsewhere ;)
        switch (user) {
            case 'ltearno':
                break

            case 'ayoka':
                refs = refs.filter(ref => {
                    switch (ref) {
                        case 'CLIENT_MUSIQUE':
                        case 'CLIENT_PHOTOS':
                        case 'CLIENT_VIDEOS':
                            return true
                        default:
                            return false
                    }
                })
                break

            case 'alice.gallas':
                refs = refs.filter(ref => {
                    switch (ref) {
                        case 'CLIENT_POUR-MAMAN':
                        case 'CLIENT_MUSIQUE':
                            return true
                        default:
                            return false
                    }
                })
                break

            default:
                refs = []
                break
        }

        return refs
    }
    catch (err) {
        return []
    }
}