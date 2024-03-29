import { LoggerBuilder } from '@ltearno/hexa-js'
import * as Model from './Model.js'

const log = LoggerBuilder.buildLogger('db-helpers')

export interface DbParams {
    host: string
    database: string
    user: string
    password: string
    port: number
}

let openedClients = new Map<any, string>()

export async function createClient(options: {
    host: string
    database: string
    user: string
    password: string
    port?: number
}, cause: string) {
    const { Client } = require('pg')

    const client = new Client({
        host: options.host,
        port: options.port || 5432,
        database: options.database,
        user: options.user,
        password: options.password,
    })

    await client.connect()

    openedClients.set(client, cause)
    printClientStats(`created DB client`)

    return client
}

export function closeClient(client) {
    if (!client)
        return

    client.end()

    openedClients.delete(client)
    printClientStats(`closed DB client`)
}

function printClientStats(cause: string) {
    let stats: any = {}
    for (let origin of openedClients.values()) {
        stats[origin] = (stats[origin] || 0) + 1
    }
    log(`${cause}, currently opened: ${openedClients.size} ${JSON.stringify(stats)}`)
}

export interface DbCursor {
    read(nb: number): Promise<any[]>
    close(): Promise<any>
}

export async function createCursor(client: any, query: string): Promise<DbCursor> {
    const Cursor = require('pg-cursor')

    const cursor = client.query(new Cursor(query))

    return Promise.resolve({
        read: async (nb: number): Promise<any[]> => {
            return new Promise((resolve, reject) => {
                try {
                    cursor.read(nb, function (err, rows) {
                        if (err) {
                            reject(err)
                            return
                        }

                        resolve(rows)
                    })
                }
                catch (err) {
                    reject(err)
                }
            })
        },

        close: () => new Promise(resolve => {
            cursor.close(resolve)
        })
    })
}

export async function insertObjectHierarchy(client, sourceId: string, parentSha: string, file: Model.FileDescriptor, mimeType: string) {
    if (!file)
        return

    if (!file.contentSha) {
        log.err(`no sha specified when inserting object ${JSON.stringify(file)}`)
        return
    }

    let fileName = file.name.replace('\\', '/')
    let mimeTypeType = mimeType.split('/')[0]
    let mimeTypeSubType = mimeType.split('/')[1]

    log.dbg(`insert object_hierarchy ${file.contentSha}`)

    await dbQuery(client, {
        text: 'INSERT INTO objects_hierarchy(sourceId, parentSha, sha, size, lastWrite, name, mimeTypeType, mimeTypeSubType) VALUES($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING',
        values: [sourceId, parentSha, file.contentSha, file.size, file.lastWrite, fileName, mimeTypeType, mimeTypeSubType],
    })
}

export async function hasObjectHierarchy(client, sha: string, sourceId: string) {
    let results: any = await dbQuery(client, `select sha, sourceId from objects_hierarchy where sha='${sha}' and sourceId='${sourceId}' limit 1;`)
    return !!(results && results.rows && results.rows.length)
}

export async function deleteObjectsHierarchyFromSource(client, sourceId: string) {
    log.dbg(`remove objects hierarchy from source ${sourceId}`)

    await dbQuery(client, {
        text: 'DELETE FROM objects_hierarchy WHERE sourceId=$1',
        values: [sourceId],
    })
}

export async function insertObjectFootprint(client, sha: string, footprint: string) {
    if (!sha || !footprint)
        return

    await dbQuery(client, {
        text: `INSERT INTO object_footprints(sha, footprint) VALUES($1, $2) ON CONFLICT (sha) DO UPDATE SET footprint=$2;`,
        values: [sha, footprint],
    })
}

export async function insertObjectAudioTags(client, sha: string, tags: object, footprint: string) {
    if (!sha || !tags)
        return

    await dbQuery(client, {
        text: `INSERT INTO object_audio_tags(sha, tags, footprint) VALUES($1, $2, $3) ON CONFLICT (sha) DO UPDATE SET tags=$2, footprint=$3;`,
        values: [sha, JSON.stringify(tags).replace(/\0/g, '').replace(/\\u0000/g, ''), footprint],
    })
}

export async function insertObjectExif(client, sha: string, exif: object, type: string, date: Date, model, height, width, latitude, latitudeRef, longitude, longitudeRef) {
    if (!sha || !exif)
        return

    await dbQuery(client, {
        text: `INSERT INTO object_exifs(sha, exif, type, date, model, height, width, latitude, latitudeRef, longitude, longitudeRef) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (sha) DO UPDATE SET exif=$2, type=$3, date=$4, model=$5, height=$6, width=$7, latitude=$8, latitudeRef=$9, longitude=$10, longitudeRef=$11;`,
        values: [
            sha,
            JSON.stringify(exif),
            type,
            date ? date.toISOString().slice(0, 16).replace('T', ' ') : null,
            model,
            height,
            width,
            latitude,
            latitudeRef,
            longitude,
            longitudeRef],
    })
}

export async function dbQuery(client, query): Promise<any> {
    return new Promise((resolve, reject) => {
        client.query(query, (err, res) => {
            if (err)
                reject(err)
            else
                resolve(res)
        })
    })
}

