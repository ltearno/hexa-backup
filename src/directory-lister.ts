import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as Stream from 'stream'

const log = require('./Logger')('DirectoryLister')

export class DirectoryLister extends Stream.Readable {
    private stack: string[]

    constructor(private path: string, private ignoredNames: string[]) {
        super({ objectMode: true })

        this.stack = [this.path]
    }

    private duringRead = false

    _read(size: number) {
        if (this.duringRead) {
            log(`OllyCow`)
            return
        }
        this.duringRead = true

        let pushed = 0
        while (this.stack.length > 0 && pushed == 0) {
            let currentPath = this.stack.shift()

            let toAdd = []

            try {
                let files = FsTools.readDirSync(currentPath)
                files
                    .filter((fileName) => !this.ignoredNames.some(name => fileName == name))
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
                    .filter(p => p != null)
                    .forEach(desc => {
                        if (desc.isDirectory)
                            toAdd.push(desc.name)

                        pushed++
                        this.push(desc)
                    })
            }
            catch (error) {
                log(`cannot read ${currentPath}`)
            }

            for (let i = toAdd.length - 1; i >= 0; i--)
                this.stack.unshift(toAdd[i])
        }

        if (this.stack.length == 0)
            this.push(null)

        this.duringRead = false
    }
}