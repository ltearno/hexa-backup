import * as MimeTypes from './mime-types'
import * as Model from './Model'

export function getFileMimeType(fileName: string) {
    let pos = fileName.lastIndexOf('.')
    if (pos >= 0) {
        let extension = fileName.substr(pos + 1).toLocaleLowerCase()
        if (extension in MimeTypes.MimeTypes)
            return MimeTypes.MimeTypes[extension]
    }

    return 'application/octet-stream'
}

export async function insertObject(client, file: Model.FileDescriptor) {
    if (!file)
        return

    let fileName = file.name.replace('\\', '/')
    let mimeType = file.isDirectory ? 'application/directory' : getFileMimeType(fileName)

    await dbQuery(client, {
        text: 'INSERT INTO objects(sha, isDirectory, size, lastWrite, name, mimeType) VALUES($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING',
        values: [file.contentSha, file.isDirectory, file.size, file.lastWrite, fileName, mimeType],
    })
}

export async function insertObjectSource(client, sha: string, sourceId: string) {
    if (!sha)
        return

    await dbQuery(client, {
        text: 'INSERT INTO object_sources(sha, sourceId) VALUES($1, $2) ON CONFLICT DO NOTHING',
        values: [sha, sourceId],
    })
}

export async function insertObjectParent(client, sha: string, parentSha: string) {
    if (!sha)
        return

    await dbQuery(client, {
        text: 'INSERT INTO object_parents(sha, parentSha) VALUES($1, $2) ON CONFLICT DO NOTHING',
        values: [sha, parentSha],
    })
}

export async function insertObjectExif(client, sha: string, exif: object) {
    if (!sha || !exif)
        return

    await dbQuery(client, {
        text: 'INSERT INTO object_exifs(sha, exif) VALUES($1, $2) ON CONFLICT DO NOTHING',
        values: [sha, JSON.stringify(exif)],
    })
}

export async function hasObjectSource(client, sha: string, sourceId: string) {
    let results: any = await dbQuery(client, `select sha, sourceId from object_sources where sha='${sha}' and sourceId='${sourceId}' limit 1;`)
    return !!(results && results.rows && results.rows.length)
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
