import * as fs from 'fs'
import * as fsPath from 'path'

import { LoggerBuilder } from '@ltearno/hexa-js'

const level = require('level')

export class Server {
    private log = LoggerBuilder.buildLogger('metadata-server')
    private db: any

    constructor(rootDirectory: string) {
        let dbDirectory = fsPath.join(rootDirectory, '.hb-metadata')
        if (!fs.existsSync(dbDirectory))
            fs.mkdirSync(dbDirectory)

        try {
            let dbFileName = fsPath.join(dbDirectory, 'data.level.db')
            this.db = level(dbFileName)
        }
        catch (error) {
            this.db = null
        }
    }

    init(app) {
        app.get('/metadata/:name', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let name = req.params.name
            if (name == null || name.trim() == 'null') {
                res.send(JSON.stringify({ error: `input validation (name is ${name}` }))
                return
            }

            let queryRaw = req.query.q
            if (queryRaw && queryRaw.trim().length) {
                let query = JSON.parse(queryRaw)
                if (query && query.shaList) {
                    let result = {}
                    for (let sha of query.shaList) {
                        try {
                            result[sha] = await this.db.get(`/metadata/${name}/${sha}`)
                        } catch (err) {
                        }
                    }

                    res.send(JSON.stringify(result))
                    return
                }
            }

            const options = {
                gte: `/metadata/${name}/`,
                lt: `/metadata/${name}0`,
                limit: 1000
            }

            let data = {}

            this.db.createReadStream(options)
                .on('data', d => data[d.key.substr(d.key.lastIndexOf('/') + 1)] = JSON.parse(d.value))
                .on('error', e => res.send(JSON.stringify({ error: `error fetching data ${e}` })))
                .on('end', () => res.send(JSON.stringify(data)))
        })

        app.get('/metadata/:name/:sha', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let name = req.params.name
            let sha = req.params.sha
            if (name == null || name.trim() == 'null' || sha == null || sha.trim() == 'null') {
                res.send(JSON.stringify({ error: `input validation (name or sha is ${name}` }))
                return
            }

            try {
                let raw = await this.db.get(`/metadata/${name}/${sha}`)
                res.send(raw)
            }
            catch (err) {
                res.send("null")
            }
        })

        app.post('/metadata/:name/:sha', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let name = req.params.name
            let sha = req.params.sha
            let value = req.body
            if (!value)
                value = {}

            if (name == null || name.trim() == 'null' || sha == null || sha.trim() == 'null') {
                res.send(JSON.stringify({ error: `input validation (name or sha is ${name}` }))
                return
            }

            await this.db.put(`/metadata/${name}/${sha}`, JSON.stringify(value))

            res.send(JSON.stringify({ message: `ok` }))
        })
    }
}