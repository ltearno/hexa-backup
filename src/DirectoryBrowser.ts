import * as fsPath from 'path'
import * as fs from 'fs'
import * as Model from './Model'
import { LoggerBuilder, Queue, FsTools, HashTools, OrderedJson } from '@ltearno/hexa-js'
import { ShaCache } from './ShaCache';

const log = LoggerBuilder.buildLogger('directory-browser')

export interface OpenedFileEntry {
    type: 'file'
    fullPath: string
    size: number
}

export interface OpenedDirectoryEntry {
    type: 'directory'
    descriptorRaw: string
    size: number
}

export type OpenedEntry = OpenedDirectoryEntry | OpenedFileEntry

export class DirectoryBrowser {
    private openedEntries = new Map<string, OpenedEntry>()

    constructor(private rootPath: string, private pusher: Queue.Pusher<Model.FileDescriptor>, private shaCache: ShaCache) {
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
            /^\.hb-cache$|^.*\\\.hb-cache$/gi,
            /^\.hb-object$|^.*\\\.hb-object$/gi,
            /^\.hb-refs$|^.*\\\.hb-refs$/gi
        ]

        let d = await this.walkDir(this.rootPath, ignoreExpressions)
        return d.sha
    }

    closeEntry(sha: string): OpenedEntry {
        let res = this.openedEntries.get(sha)
        this.openedEntries.delete(sha)
        return res
    }

    async walkDir(path: string, ignoreExpressions: any[]): Promise<{ descriptor: Model.DirectoryDescriptor; sha: string; size: number }> {
        try {
            let hbIgnorePath = fsPath.join(path, '.hbignore')
            if (fs.existsSync(hbIgnorePath)) {
                ignoreExpressions = ignoreExpressions.slice()
                let lines = fs
                    .readFileSync(hbIgnorePath, 'utf8')
                    .split(/\r\n|\n\r|\n|\r/g)
                    .filter(line => !line.startsWith('#') && line.trim().length)
                lines.forEach(line => {
                    try {
                        let regexp = new RegExp(line, 'ig')
                        log(`ignoring pattern ${line}`)
                        ignoreExpressions.push(regexp)
                    }
                    catch (error) {
                        log.err(`error in ${hbIgnorePath} at regexp ${line} : ${error}`)
                    }
                })
            }

            let files = (await FsTools.readDir(path))
                .sort()
                .map(fileName => fsPath.join(path, fileName))
                .filter(fileName => {
                    let relative = fsPath.basename(fileName)
                    let ignores = ignoreExpressions.some(expression => expression.test(relative))
                    log.dbg(`test ${relative} => ${ignores}`)
                    if (ignores)
                        log.dbg(`ignored path ${fileName}`)
                    return !ignores
                })
                .map(fileName => {
                    try {
                        let stat = fs.lstatSync(fileName)
                        if (!stat)
                            return null

                        if (stat.isSymbolicLink()) {
                            log.wrn(`skipped symbolic link ${fileName}`)
                            return null
                        }

                        if (!(stat.isDirectory() || stat.isFile())) {
                            log.wrn(`skipped ${fileName}`)
                            return null
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

                        return element
                    }
                    catch (error) {
                        log.wrn(`cannot stat ${fileName}`)
                        return null
                    }
                })
                .filter(desc => desc != null)

            let directoryDescriptor: Model.DirectoryDescriptor = {
                files: []
            }

            for (let desc of files) {
                try {
                    if (desc.isDirectory) {
                        let subDirectoryDescriptor = await this.walkDir(desc.name, ignoreExpressions)
                        if (subDirectoryDescriptor) {
                            directoryDescriptor.files.push({
                                name: fsPath.basename(desc.name),
                                isDirectory: true,
                                lastWrite: desc.lastWrite,
                                contentSha: subDirectoryDescriptor.sha,
                                size: subDirectoryDescriptor.size
                            })
                        }
                    }
                    else {
                        let startTime = Date.now()

                        const fullPath = desc.name
                        let fileSha = await this.shaCache.hashFile(fullPath)
                        let entry = {
                            name: fsPath.basename(desc.name),
                            isDirectory: false,
                            lastWrite: desc.lastWrite,
                            contentSha: fileSha,
                            size: desc.size
                        }

                        this.stats.timeHashing += Date.now() - startTime
                        this.stats.bytesHashed += desc.size

                        directoryDescriptor.files.push(entry)

                        this.openedEntries.set(fileSha, { type: 'file', fullPath, size: desc.size })
                        await this.pusher(entry)
                    }
                }
                catch (error) {
                    log.err(`error browsing ${JSON.stringify(desc)} : ${error}`)
                }
            }

            let directoryDescriptorRaw = OrderedJson.stringify(directoryDescriptor)
            let directoryDescriptorSha = await HashTools.hashString(directoryDescriptorRaw)

            this.openedEntries.set(directoryDescriptorSha, { type: 'directory', descriptorRaw: directoryDescriptorRaw, size: Buffer.from(directoryDescriptorRaw, 'utf8').length })

            let stat = fs.statSync(path)
            await this.pusher({
                name: '',
                isDirectory: true,
                lastWrite: stat.mtime.getTime(),
                contentSha: directoryDescriptorSha,
                size: directoryDescriptorRaw.length
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
