import fs = require('fs')
import fsPath = require('path')
import * as FsTools from './FsTools'
import * as Stream from 'stream'

const log = require('./Logger')('DirectoryLister')

interface FileIteration {
    name: string
    isDirectory: boolean
    lastWrite: number
    size: number
}

function* iterateRecursivelyOverDirectory(path: string, stateCallback?: (nbFiles: number, nbDirs: number) => void): IterableIterator<FileIteration> {
    let stack = [path]

    let ignoreExpressions: RegExp[] = [
        /^\.hb-cache$|^.*\\\.hb-cache$/gi,
        /^\.hb-object$|^.*\\\.hb-object$/gi,
        /^\.hb-refs$|^.*\\\.hb-refs$/gi
    ]

    let nbFiles = 0
    let nbDirectories = 0

    while (stack.length) {
        let currentPath = stack.pop()

        try {
            let hbIgnorePath = fsPath.join(currentPath, '.hbignore')
            if (fs.existsSync(hbIgnorePath)) {
                let lines = fs
                    .readFileSync(hbIgnorePath, 'utf8')
                    .split(/\r\n|\n\r|\n|\r/g)
                    .filter(line => !line.startsWith('#') && line.trim().length)
                //.map(line => //fsPath.relative(path, fsPath.join(currentPath, line)))
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

            let files = FsTools.readDirSync(currentPath)
                .sort()
                .map(fileName => fsPath.join(currentPath, fileName))
                .filter(fileName => {
                    let relative = fsPath.relative(path, fileName)
                    let ignores = ignoreExpressions.some(expression => expression.test(relative))
                    if (ignores)
                        log.dbg(`ignored ${fileName}`)
                    return !ignores
                })
                .map(fileName => {
                    try {
                        let stat = fs.statSync(fileName)

                        return {
                            name: fileName,
                            isDirectory: stat.isDirectory(),
                            lastWrite: stat.mtime.getTime(),
                            size: stat.isDirectory() ? 0 : stat.size
                        }
                    }
                    catch (error) {
                        log(`cannot stat ${fileName}`)
                        return null
                    }
                })
                .filter(desc => desc != null)

            for (let desc of files) {
                if (desc.isDirectory) {
                    stack.push(desc.name)
                    nbDirectories++
                }
                else {
                    nbFiles++
                }

                yield desc
            }

            stateCallback && stateCallback(nbFiles, nbDirectories)
        }
        catch (error) {
            log(`cannot read ${currentPath}`)
        }
    }
}

export class DirectoryLister extends Stream.Readable {
    private fileIterator: IterableIterator<FileIteration>

    constructor(private path: string, stateCallback?: (nbFiles: number, nbDirs: number) => void) {
        super({ objectMode: true })

        this.fileIterator = iterateRecursivelyOverDirectory(path, stateCallback)
    }

    async _read(size: number) {
        let result = this.fileIterator.next()
        if (result.done)
            this.push(null)
        else
            this.push(result.value)
    }
}