import { HexaBackupStore } from '../HexaBackupStore.js'
import { LoggerBuilder, HashTools } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization.js'
import { EMPTY_PAYLOAD_SHA } from '@ltearno/hexa-js/dist/hash-tools.js'

const log = LoggerBuilder.buildLogger('base-server')

export class Base {
    constructor(private store: HexaBackupStore) {
    }

    addEnpointsToApp(app: any) {
        app.get('/refs', async (req, res) => {
            res.set('Content-Type', 'application/json')

            try {
                let refs = await Authorization.getAuthorizedRefsFromHttpRequest(req, this.store)
                res.send(JSON.stringify(refs))
            }
            catch (err) {
                res.send(`{"error":"${err}"}`)
            }
        })

        app.get('/refs/:id', async (req, res) => {
            res.set('Content-Type', 'application/json')

            try {
                let id = req.params.id

                let refs = await Authorization.getAuthorizedRefsFromHttpRequest(req, this.store)
                if (!refs || !refs.includes(id)) {
                    res.send(`{"error":"not authorized"}`)
                    return null
                }

                let result = await this.store.getSourceState(id)

                res.send(JSON.stringify(result))
            }
            catch (err) {
                res.send(`{"error":"${err}"}`)
            }
        })

        app.post('/refs/:id/directory_descriptor/:desc', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let sourceId = req.params.id
            let directoryDescriptorSha = req.params.desc

            let commitSha = await this.store.registerNewCommit(sourceId, directoryDescriptorSha)

            res.send(JSON.stringify({
                reference: sourceId,
                commitSha
            }))
        })

        app.post('/refs/:id/commit/:commit', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let sourceId = req.params.id
            let commitSha = req.params.commit

            commitSha = await this.store.setSourceCommit(sourceId, commitSha)

            res.send(JSON.stringify({
                reference: sourceId,
                commitSha
            }))
        })

        app.post('/refs/:id/tags/:name', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let sourceId = req.params.id
            let tagName = req.params.name
            let tagValue = req.body

            let source = await this.store.setSourceTag(sourceId, tagName, tagValue)
            if (!source) {
                res.send(JSON.stringify({ error: `source does not exist` }))
                return
            }

            res.send(JSON.stringify(source))
        })

        app.get('/references', async (req, res) => {
            res.set('Content-Type', 'application/json')

            try {
                let result = []
                let refs = await Authorization.getAuthorizedRefsFromHttpRequest(req, this.store)
                for (let ref of refs) {
                    result.push({ name: ref, data: await this.store.getSourceState(ref) })
                }
                res.send(JSON.stringify(result))
            }
            catch (err) {
                res.send(`{"error":"${err}"}`)
            }
        })

        app.get('/sha/:sha/content', (req, res) => this.serveShaContent(req, res))

        app.get('/sha/:sha/info', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let sha = req.params.sha
            let size = await this.store.hasOneShaBytes(sha)

            res.send(JSON.stringify({
                sha,
                size
            }))
        })

        app.get('/sha/:sha/verify', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let sha = req.params.sha
            log(`validation of sha '${sha}'`)
            let size = await this.store.hasOneShaBytes(sha)
            let verified = false
            if (!size)
                verified = sha == EMPTY_PAYLOAD_SHA
            else
                verified = await this.store.validateShaBytes(sha)

            res.send(JSON.stringify({
                sha,
                size,
                verified
            }))
        })

        // phantomName is just an easy way for a link to include the desired file name
        // if the browser wants to download the file (because of mimetype), the file
        // will have the 'phantomName' instead of 'content
        app.get('/sha/:sha/content/:phantomName', (req, res) => this.serveShaContent(req, res))

        app.post('/sha', async (req, res) => {
            let bytes = req.body
            let sha = HashTools.hashBytes(bytes)
            let len = await this.store.putShaBytes(sha, 0, bytes)
            res.set('Content-Type', 'application/json')
            res.send(JSON.stringify({ sha, len }))
        })

        app.post('/sha/:sha/content/:offset', async (req, res) => {
            let sha = req.params.sha
            let offset = req.params.offset * 1

            let raw = req.body

            let written = await this.store.putShaBytes(sha, offset, raw)

            res.set('Content-Type', 'application/json')
            res.send(JSON.stringify({
                sha,
                written,
                received: raw.length
            }))
        })
    }

    private async serveShaContent(req, res) {
        res.set('Content-Type', 'application/json')

        let sha = req.params.sha
        if (sha == null || sha == 'null') {
            res.send(JSON.stringify({ error: `input validation (sha is ${sha})` }))
            return
        }

        // TODO maybe not consider knowing the sha is knowing the existence hence having permission,
        // and thus make a check against authorized refs.

        const range = req.headers.range

        try {
            const fileSize = await this.store.hasOneShaBytes(sha)

            if (range) {
                const parts = range.replace(/bytes=/, "").split("-")
                const start = parseInt(parts[0])
                const end = Math.max(start, parts[1] ? parseInt(parts[1], 10) : start == 0 ? Math.min(fileSize - 1, 100 * 1024) : fileSize - 1)
                const chunksize = (end - start) + 1
                const head = {
                    'Content-Range': `bytes ${start}-${end}/${fileSize}`,
                    'Accept-Ranges': 'bytes',
                    'Content-Length': chunksize,
                    'Content-Type': req.query.type,
                    'Cache-Control': 'private, max-age=31536000',
                    'ETag': sha
                }

                if (req.query.fileName)
                    head['Content-Disposition'] = `attachment; filename="${req.query.fileName}"`

                res.writeHead(206, head)
                this.store.readShaAsStream(sha, start, end).pipe(res)

                log.dbg(`range-rq ${sha} ${start}-${end}(${parts[1]})/${fileSize}`)
            }
            else {
                if (req.query.type)
                    res.set('Content-Type', req.query.type)

                if (req.query.fileName)
                    res.set('Content-Disposition', `attachment; filename="${req.query.fileName}"`)

                res.set('ETag', sha)
                res.set('Cache-Control', 'private, max-age=31536000')
                res.set('Content-Length', fileSize)

                this.store.readShaAsStream(sha, 0, -1).pipe(res)
            }
        }
        catch (err) {
            try {
                log.err(`error when sending byte range: ${err}`)
                res.send(`{"error":"missing sha ${sha}!"}`)
            }
            catch (err2) {
                log.err(`error when sending response: ${err2}`)
            }
        }
    }
}