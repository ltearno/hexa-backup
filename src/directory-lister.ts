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

function* iterateRecursivelyOverDirectory(path: string): IterableIterator<FileIteration> {
    let stack = [path]

    let ignoreExpressions: RegExp[] = []

    while (stack.length) {
        let currentPath = stack.pop()

        try {
            let hbIgnorePath = fsPath.join(currentPath, '.hbignore')
            if (fs.existsSync(hbIgnorePath)) {
                let lines = fs.readFileSync(hbIgnorePath, 'utf8')
                    .split(/\r\n|\n\r|\n|\r/g)
                    .filter(line => !line.startsWith('#') && line.trim().length)
                    .map(line => fsPath.relative(path, fsPath.join(currentPath, line)))
                lines.forEach(line => ignoreExpressions.push(new RegExp(line, 'i')))
            }

            let files = FsTools.readDirSync(currentPath)
                .sort()
                .map(fileName => fsPath.join(currentPath, fileName))
                .filter(fileName => {
                    let relative = fsPath.relative(path, fileName)
                    let ignores = ignoreExpressions.some(expression => expression.test(relative))
                    if (ignores)
                        log(`ignored ${fileName}`)
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
                if (desc.isDirectory)
                    stack.push(desc.name)

                yield desc
            }
        }
        catch (error) {
            log(`cannot read ${currentPath}`)
        }
    }
}

export class DirectoryLister extends Stream.Readable {
    private fileIterator: IterableIterator<FileIteration>

    constructor(private path: string) {
        super({ objectMode: true })

        this.fileIterator = iterateRecursivelyOverDirectory(path)
    }

    _read(size: number) {
        while (true) {
            let result = this.fileIterator.next()
            if (result.done) {
                this.push(null)
                break
            }

            if (!this.push(result.value))
                break
        }
    }
}