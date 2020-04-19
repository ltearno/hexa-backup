import * as fs from 'fs'
import { HexaBackupStore } from '../HexaBackupStore'
import { LoggerBuilder } from '@ltearno/hexa-js'
import * as VideoConverter from '../VideoConverter'
import { BackgroundJobs } from '../BackgroundJobs';

export class Plugins {
    thumbnailCache = new Map<string, Buffer>()
    thumbnailCacheEntries = []

    mediumCache = new Map<string, Buffer>()
    mediumCacheEntries = []

    private log = LoggerBuilder.buildLogger('plugins-server')

    private videoConverter: VideoConverter.VideoConverter

    constructor(private store: HexaBackupStore, backgroundJobs: BackgroundJobs) {
        this.videoConverter = new VideoConverter.VideoConverter(this.store, backgroundJobs)
    }

    addEnpointsToApp(app: any) {
        app.get('/sha/:sha/plugins/image/thumbnail', async (req, res) => {
            let sha = req.params.sha
            if (sha == null || sha == 'null') {
                res.set('Content-Type', 'application/json')
                res.send(`{"error":"input validation (sha is ${sha})"}`)
                return
            }

            try {
                if (req.query.type)
                    res.set('Content-Type', req.query.type)

                let out = null
                if (this.thumbnailCache.has(sha)) {
                    out = this.thumbnailCache.get(sha)
                }
                else {
                    let input = await this.store.readShaBytes(sha, 0, -1)

                    const sharp = require('sharp')

                    out = await sharp(input).rotate().resize(150).toBuffer()
                    this.thumbnailCache.set(sha, out)
                    this.thumbnailCacheEntries.push(sha)
                }

                res.set('ETag', sha)
                res.set('Cache-Control', 'private, max-age=31536000')
                res.send(out)

                if (this.thumbnailCache.size > 200) {
                    while (this.thumbnailCacheEntries.length > 50) {
                        this.thumbnailCache.delete(this.thumbnailCacheEntries.shift())
                    }
                }
            }
            catch (err) {
                this.log(`error creating a thumbnail for sha ${sha} : ${err}`)
                res.set('Content-Type', 'application/json')
                res.send(`{"error":"missing sha ${sha}"}`)
            }
        });

        app.get('/sha/:sha/plugins/image/medium', async (req, res) => {
            let sha = req.params.sha
            if (sha == null || sha == 'null') {
                res.set('Content-Type', 'application/json')
                res.send(`{"error":"input validation (sha is ${sha})"}`)
                return
            }

            try {
                if (req.query.type)
                    res.set('Content-Type', req.query.type)

                let out = null
                if (this.mediumCache.has(sha)) {
                    out = this.mediumCache.get(sha)
                }
                else {
                    let input = await this.store.readShaBytes(sha, 0, -1)

                    const sharp = require('sharp')

                    out = await sharp(input).rotate().resize(1024).toBuffer()
                    this.mediumCache.set(sha, out)
                    this.mediumCacheEntries.push(sha)
                }

                res.set('ETag', sha)
                res.set('Cache-Control', 'private, max-age=31536000')
                res.send(out)

                if (this.mediumCache.size > 20) {
                    while (this.mediumCacheEntries.length > 5) {
                        this.mediumCache.delete(this.mediumCacheEntries.shift())
                    }
                }
            }
            catch (err) {
                res.set('Content-Type', 'application/json')
                res.send(`{"error":"missing sha ${sha}!"}`)
            }
        })

        app.get('/sha/:sha/plugins/video/small', async (req, res) => {
            let sha = req.params.sha
            if (sha == null || sha == 'null') {
                res.set('Content-Type', 'application/json')
                res.send(`{"error":"input validation (sha is ${sha})"}`)
                return
            }

            try {
                let fileName = await this.videoConverter.createSmallVideo(sha)
                if (!fileName) {
                    res.set('Content-Type', 'application/json')
                    res.send(`{"error":"converting video content (sha is ${sha})"}`)
                    return
                }

                res.set('Content-Type', 'video/mp4')

                const range = req.headers.range

                let stat = fs.statSync(fileName)
                const fileSize = stat.size

                if (range) {
                    const parts = range.replace(/bytes=/, "").split("-")
                    const start = parseInt(parts[0])
                    const end = Math.max(start, parts[1] ? parseInt(parts[1], 10) : fileSize - 1)
                    const chunksize = (end - start) + 1
                    const head = {
                        'Content-Range': `bytes ${start}-${end}/${fileSize}`,
                        'Accept-Ranges': 'bytes',
                        'Content-Length': chunksize,
                        'Content-Type': 'video/mp4',
                    }

                    if (req.query.fileName)
                        head['Content-Disposition'] = `attachment; filename="${req.query.fileName}"`

                    res.writeHead(206, head)
                    fs.createReadStream(fileName, { start, end }).pipe(res)
                }
                else {
                    if (req.query.type)
                        res.set('Content-Type', 'video/mp4')

                    if (req.query.fileName)
                        res.set('Content-Disposition', `attachment; filename="${req.query.fileName}"`)

                    res.set('Cache-Control', 'private, max-age=31536000')
                    res.set('Content-Length', fileSize)

                    fs.createReadStream(fileName).pipe(res)
                }
            }
            catch (err) {
                res.set('Content-Type', 'application/json')
                try {
                    this.log.err(`error when sending byte range: ${err}`)
                    res.send(`{"error":"missing sha ${sha}!"}`)
                }
                catch (err2) {
                    this.log.err(`error when sending response: ${err2}`)
                }
            }
        })
    }
}