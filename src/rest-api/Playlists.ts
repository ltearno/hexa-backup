import { HexaBackupStore } from '../HexaBackupStore'
import { LoggerBuilder, OrderedJson, HashTools } from '@ltearno/hexa-js'
import * as Authorization from '../Authorization'
import * as Model from '../Model'

const log = LoggerBuilder.buildLogger('plugins-server')

function userSourceId(user: string, name: string) {
    if (!user || !name || name.length > 200)
        return `plugin-playlists-common`
    return `plugin-playlists-${user}-${name}`
}

function userSourcePrefix(user: string) {
    if (!user)
        return null
    return `plugin-playlists-${user}-`
}

export class Playlists {
    constructor(private store: HexaBackupStore) {
    }

    addEnpointsToApp(app: any) {
        app.get('/plugins/playlists', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let user = await Authorization.getUserFromRequest(req)
            let refs = await Authorization.getAuthorizedRefsFromHttpRequest(req, this.store)
            if (!user || !refs || !refs.length) {
                res.send(JSON.stringify({ error: `no user/refs allowed (${user}/${refs})` }))
                return
            }

            let userRefs = []

            const prefix = userSourcePrefix(user).toLocaleLowerCase()
            for (let ref of refs) {
                if (!ref.toLocaleLowerCase().startsWith(prefix))
                    continue

                userRefs.push(ref.substr(prefix.length))
            }

            res.send(JSON.stringify(userRefs))
        });

        app.get('/plugins/playlists/:name', async (req, res) => {
            try {
                res.set('Content-Type', 'application/json')

                let user = await Authorization.getUserFromRequest(req)
                if (!user) {
                    res.send(JSON.stringify({ error: `no user specified` }))
                    return
                }

                const playlistName = req.params.name
                const playlistId = userSourceId(user, playlistName)

                const state = await this.store.getSourceState(playlistId)
                if (!state || !state.currentCommitSha) {
                    res.send(JSON.stringify({ error: `no state/commit ${playlistId}` }))
                    return
                }

                const commit = await this.store.getCommit(state.currentCommitSha)

                const playlistDirectoryDescriptor = JSON.parse((await this.store.readShaBytes(commit.directoryDescriptorSha, 0, -1)).toString('utf8')) as Model.DirectoryDescriptor

                let result: PlaylistRequest = {
                    name: req.params.name,
                    descriptor: []
                };

                for (let file of playlistDirectoryDescriptor.files) {
                    result.descriptor.push({
                        name: file.name,
                        date: file.lastWrite,
                        isDirectory: file.isDirectory,
                        mimeType: "",
                        sha: file.contentSha
                    })
                }

                res.send(JSON.stringify(result))
            }
            catch (err) {
                log.err(`error get playlist ${err}`)
                res.send(JSON.stringify({ error: `internal` }))
            }
        })

        interface PlaylistRequest {
            name: string
            descriptor: {
                name: string
                sha: string
                isDirectory: boolean
                mimeType: string
                date: number
            }[]
        }

        interface PlaylistAddRequest {
            items: {
                name: string
                sha: string
                isDirectory: boolean
                mimeType: string
                date: number
            }[]
        }

        // todo remove from a playlist

        // add to the playlist
        app.put('/plugins/playlists/:name', async (req, res) => {
            res.set('Content-Type', 'application/json')

            let user = await Authorization.getUserFromRequest(req)
            if (!user) {
                res.send(JSON.stringify({ error: `no user specified` }))
                return
            }

            const name = req.params.name
            let request = req.body as PlaylistAddRequest

            if (!name || !name.length || name.length > 200) {
                res.send(JSON.stringify({ error: `no name specified/name too long` }))
                return
            }

            let messages = []

            // fetch playlist directory descriptor
            const playlistSourceId = userSourceId(user, name)
            let sourceState = await this.store.getSourceState(playlistSourceId)
            let commit = sourceState && sourceState.currentCommitSha && await this.store.getCommit(sourceState.currentCommitSha)
            let currentDescriptor = commit && commit.directoryDescriptorSha && await this.store.getDirectoryDescriptor(commit.directoryDescriptorSha)
            if (!currentDescriptor) {
                messages.push(`creating directory descriptor`)
                currentDescriptor = {
                    files: []
                }
            }

            // add items
            let playlistDescriptor: Model.DirectoryDescriptor = JSON.parse(JSON.stringify(currentDescriptor))
            for (let item of request.items) {
                playlistDescriptor.files.push({
                    contentSha: item.sha,
                    name: item.name,
                    isDirectory: item.isDirectory,
                    lastWrite: item.date,
                    size: 0
                })
            }

            // store new playlist directory descriptor
            let stringified = OrderedJson.stringify(playlistDescriptor)
            let playlistDescriptorRaw = Buffer.from(stringified, 'utf8')
            let playlistDescriptorSha = HashTools.hashStringSync(stringified)

            if (!await this.store.hasOneShaBytes(playlistDescriptorSha)) {
                log.dbg(`write playlist directory descriptor for user ${user}, name ${name} to ${playlistDescriptorSha}`)

                await this.store.putShaBytes(playlistDescriptorSha, 0, playlistDescriptorRaw)
                if (!await this.store.validateShaBytes(playlistDescriptorSha)) {
                    res.send(JSON.stringify({ error: `cannot validate playlist content` }))
                    return
                }
            }

            // commit the changes
            let commitSha = await this.store.registerNewCommit(playlistSourceId, playlistDescriptorSha)
            res.send(JSON.stringify({ ok: `went well, commit ${commitSha} with descriptor ${playlistDescriptorSha} descriptor ${playlistDescriptorSha}`, messages }))
        })
    }
}