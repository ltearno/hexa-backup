import { HexaBackupStore } from '../HexaBackupStore'
import { LoggerBuilder } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization'

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
        });

        app.get('/sha/:sha/content', (req, res) => this.serveShaContent(req, res))

        // phantomName is just an easy way for a link to include the desired file name
        // if the browser wants to download the file (because of mimetype), the file
        // will have the 'phantomName' instead of 'content
        app.get('/sha/:sha/content/:phantomName', (req, res) => this.serveShaContent(req, res))
    }

    private async serveShaContent(req, res) {
        res.set('Content-Type', 'application/json')

        let sha = req.params.sha
        if (sha == null || sha == 'null') {
            res.send(`{"error":"input validation (sha is ${sha})"}`)
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