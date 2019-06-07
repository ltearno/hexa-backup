import { LoggerBuilder } from '@ltearno/hexa-js'

const log = LoggerBuilder.buildLogger('PathSpecHelpers')

export function parseTargetSpec(s: string) {
    if (!s || !s.trim().length) {
        log.err(`empty target`)
        return null
    }

    if (!s.includes(':')) {
        log.err(`target should be in the form 'sourceId:path'`)
        return null
    }

    let parts = s.split(':')
    if (parts.length != 2) {
        log.err(`syntax error for target : '${s}'`)
        return null
    }

    let sourceId = parts[0]

    let path = parts[1].trim()
    if (path.startsWith('/'))
        path = path.substring(1)
    let pathParts = path.length ? path.split('/') : []

    return {
        sourceId,
        pathParts
    }
}