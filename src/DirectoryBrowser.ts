import * as fsPath from 'path'
import * as fs from 'fs'
import * as Model from './Model'
import { LoggerBuilder, Queue, FsTools, HashTools, OrderedJson } from '@ltearno/hexa-js'
import { ShaCache } from './ShaCache';

const log = LoggerBuilder.buildLogger('directory-browser')

export interface Entry {
    model: Model.FileDescriptor
    fullPath: string // absolute path
    walkPath: string // path from the root of the walk
    directoryDescriptorRaw?: string
}

function globToRegExp(glob: string): RegExp {
    let r = glob
        .replace(/\./g, '\\.')
        .replace(/\//g, '\\/')
        .replace(/\*/g, `[^\\/]+?`)
        + '$'

    log.dbg(`filter ${glob} to ${r}`)

    return new RegExp(r, 'i')
}

export class DirectoryBrowser {
    constructor(private rootPath: string, private pusher: Queue.Pusher<Entry>, private shaCache: ShaCache) {
    }

    stats = {
        nbFilesBrowsed: 0,
        nbDirectoriesBrowsed: 0,
        bytesBrowsed: 0,
        bytesHashed: 0,
        timeHashing: 0
    }

    async start() {
        let ignoreExpressions: RegExp[] = [
            "/.hb-cache",
            "/.hb-object",
            "/.hb-refs",
            "/.hb-metadata",
            "/.git",
            "/node_modules"
        ].map(globToRegExp)

        let d = await this.walkDir(this.rootPath, ignoreExpressions)

        this.pusher(null)

        return d.sha
    }

    async walkDir(path: string, ignoreExpressions: RegExp[]): Promise<{ descriptor: Model.DirectoryDescriptor; sha: string; size: number }> {
        try {
            let hbIgnorePath = fsPath.join(path, '.hb-ignore')
            if (fs.existsSync(hbIgnorePath)) {
                ignoreExpressions = ignoreExpressions.slice()
                let lines = fs
                    .readFileSync(hbIgnorePath, 'utf8')
                    .split(/\r\n|\n\r|\n|\r/g)
                    .map(line => line.trim())
                    .filter(line => !line.startsWith('#') && line.length)
                lines.forEach(line => {
                    try {
                        log(`ignoring pattern ${line}`)
                        if (line.startsWith('/')) {
                            ignoreExpressions.push(globToRegExp(fsPath.relative(this.rootPath, path) + line))
                        }
                        else {
                            ignoreExpressions.push(globToRegExp('/' + line))
                        }
                    }
                    catch (error) {
                        log.err(`error in ${hbIgnorePath} at regexp ${line} : ${error}`)
                    }
                })
            }

            let directoryDescriptor: Model.DirectoryDescriptor = {
                files: []
            }

            let rawFiles = (await FsTools.readDir(path))
                .sort()

            for (let fileName of rawFiles) {
                fileName = fsPath.join(path, fileName)

                const walkPath = '/' + fsPath.relative(this.rootPath, fileName)

                //let relative = fsPath.basename(fileName)
                let ignores = ignoreExpressions.some(expression => expression.test(walkPath))
                if (ignores) {
                    log.dbg(`ignored path ${fileName}`)
                    continue
                }

                try {
                    let stat = fs.lstatSync(fileName)
                    if (!stat)
                        continue

                    if (stat.isSymbolicLink()) {
                        let symbolicLinkTarget = fs.realpathSync(fs.readlinkSync(fileName))
                        log.wrn(`skipped symbolic link ${fileName}, targetting ${symbolicLinkTarget}`)
                        continue
                    }

                    if (!(stat.isDirectory() || stat.isFile())) {
                        log.wrn(`skipped ${fileName} (nor directory, nor file)`)
                        continue
                    }

                    let element = {
                        name: fileName,
                        isDirectory: stat.isDirectory(),
                        lastWrite: stat.mtime.getTime(),
                        size: stat.isDirectory() ? 0 : stat.size
                    }

                    this.stats.bytesBrowsed += element.size
                    if (element.isDirectory)
                        this.stats.nbDirectoriesBrowsed++
                    else
                        this.stats.nbFilesBrowsed++

                    // process element
                    try {
                        if (element.isDirectory) {
                            let subDirectoryDescriptor = await this.walkDir(element.name, ignoreExpressions)
                            if (subDirectoryDescriptor) {
                                directoryDescriptor.files.push({
                                    name: fsPath.basename(element.name),
                                    isDirectory: true,
                                    lastWrite: element.lastWrite,
                                    contentSha: subDirectoryDescriptor.sha,
                                    size: subDirectoryDescriptor.size
                                })
                            }
                        }
                        else {
                            let startTime = Date.now()

                            const fullPath = element.name
                            let fileSha = await this.shaCache.hashFile(fullPath)
                            let model = {
                                name: fsPath.basename(element.name),
                                isDirectory: false,
                                lastWrite: element.lastWrite,
                                contentSha: fileSha,
                                size: element.size
                            }

                            this.stats.timeHashing += Date.now() - startTime
                            this.stats.bytesHashed += element.size

                            directoryDescriptor.files.push(model)

                            await this.pusher({ model, fullPath, walkPath })
                        }
                    }
                    catch (error) {
                        log.err(`error browsing ${JSON.stringify(element)} : ${error}`)
                    }
                }
                catch (error) {
                    log.wrn(`cannot stat ${fileName} in ${path} (${error})`)
                }
            }

            let directoryDescriptorRaw = OrderedJson.stringify(directoryDescriptor)
            let directoryDescriptorSha = await HashTools.hashString(directoryDescriptorRaw)

            const walkPath = '/' + fsPath.relative(this.rootPath, path)

            let stat = fs.statSync(path)
            await this.pusher({
                model: {
                    name: '',
                    isDirectory: true,
                    lastWrite: stat.mtime.getTime(),
                    contentSha: directoryDescriptorSha,
                    size: directoryDescriptorRaw.length
                },
                fullPath: path,
                walkPath,
                directoryDescriptorRaw
            })

            return {
                descriptor: directoryDescriptor,
                sha: directoryDescriptorSha,
                size: directoryDescriptorRaw.length
            }
        }
        catch (error) {
            log(`cannot read ${path} ${error}`)
            return null
        }
    }
}
