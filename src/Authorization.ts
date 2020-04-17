import { IHexaBackupStore } from './HexaBackupStore'
import { LoggerBuilder } from '@ltearno/hexa-js'

const log = LoggerBuilder.buildLogger('hexa-backup')

let authorizationDisabled = false

export function disableAuthorization() {
    log.wrn(`AUTHORIZATION AND ACL DISABLED, OPEN-BAR MODE !!!`)
    authorizationDisabled = true
}

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

        if (authorizationDisabled)
            return refs

        if (user == 'ltearno')
            return refs

        let authorizedRefs = new Set<string>()

        const addFamille = () => {
            authorizedRefs.add('CLIENT_MUSIQUE')
            authorizedRefs.add('CLIENT_FAMILLE')
        }

        const addFamilleTournier = () => {
            authorizedRefs.add('CLIENT_FAMILLE-TOURNIER')
        }

        const addTribu = () => {
            authorizedRefs.add('CLIENT_PHOTOS')
            authorizedRefs.add('CLIENT_VIDEOS')
        }

        switch (user) {
            case 'ayoka':
                addFamille()
                addFamilleTournier()
                addTribu()
                break

            case 'alice.gallas':
                authorizedRefs.add('CLIENT_POUR-MAMAN')
                addFamille()
                addFamilleTournier()
                break

            case 'papa':
                addFamille()
                addFamilleTournier()
                break

            case 'fx':
                addFamille()
                addFamilleTournier()
                break

            case 'rv':
                addFamille()
                addFamilleTournier()
                break

            case 'pat':
                addFamille()
                addFamilleTournier()
                break

            case 'famille':
                addFamille()
                break

            case 'eveline':
                addFamille()
                break

            case 'virginie':
                addFamille()
                break

            case 'manou':
                addFamille()
                break
        }

        return refs.filter(ref => authorizedRefs.has(ref))
    }
    catch (err) {
        return []
    }
}