import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as Stream from 'stream'

const log = require('./Logger')('DirectoryLister')

interface FileIteration {
    name: string
    isDirectory: boolean
    lastWrite: number
    size: number
}

function* iterateRecursivelyOverDirectory(path: string, ignoredNames: string[]): IterableIterator<FileIteration> {
    let stack = [path]

    while (stack.length) {
        let currentPath = stack.shift()

        try {
            let files = FsTools.readDirSync(currentPath)
                .filter((fileName) => !ignoredNames.some(name => fileName == name))
                .sort()
                .map(fileName => fsPath.join(currentPath, fileName))
                .map(fileName => {
                    try {
                        let stat = fs.statSync(fileName);

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
                    stack.unshift(desc.name)

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

    constructor(private path: string, private ignoredNames: string[]) {
        super({ objectMode: true })

        this.fileIterator = iterateRecursivelyOverDirectory(path, ignoredNames)
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