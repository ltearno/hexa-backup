import fs = require('fs')
import os = require('os')
import fsPath = require('path')
import { readFileContent, lstat } from '../FsTools'

import { IHexaBackupStore, HexaBackupStore } from '../HexaBackupStore'
import { HexaBackupReader } from '../HexaBackupReader'
import { RPCClient, RPCServer } from '../RPC'
import * as Commands from '../Commands'

const log = require('../Logger')('hexa-backup')
log.conf('dbg', false)

function parseArgs(args: string[], defaultParameters): { verbs: string[]; parameters: { [k: string]: any } } {
    let parameters = {}

    if (defaultParameters)
        for (let k in defaultParameters)
            parameters[k] = defaultParameters[k]

    // verb/noun
    // -key value
    // --flag (=== key:true)

    let verbs = []
    for (let i = 0; i < args.length; i++) {
        let token = args[i];
        if (token.charAt(0) == '-' && token.charAt(1) == '-') {
            parameters[token.slice(2)] = true
        }
        else if (token.charAt(0) == '-') {
            parameters[token.slice(1)] = args[i + 1]
            i++
        }
        else {
            verbs.push(token)
        }
    }

    return {
        verbs: verbs,
        parameters: parameters
    }
}

async function run() {
    let args = process.argv.slice(2)
    log(JSON.stringify(args))

    let defaultParameters = null
    const parametersFileName = '.hexa-backup.json'
    try {
        let stat = await lstat(parametersFileName);
        if (stat != null) {
            let content = await readFileContent(parametersFileName, 'utf8')
            defaultParameters = JSON.parse(content) || {}
        }
    }
    catch (error) {
    }

    let defaultSourceId = `${os.hostname()}-${fsPath.basename(process.cwd())}`

    let cmdManager = new CommandManager([
        {
            id: "history",
            verbs: ["history"],
            options: {
                sourceId: defaultSourceId,
                storeIp: "localhost",
                storePort: 5005
            },
            executor: async (options) => {
                const sourceId = options['sourceId']
                const storeIp = options['storeIp']
                const storePort = options['storePort']

                await Commands.history(sourceId, storeIp, storePort)

                process.exit(0)
            }
        },
        {
            id: "showCurrentTransaction",
            verbs: ["show", "current", "?prefix"],
            options: {
                sourceId: defaultSourceId,
                storeIp: "localhost",
                storePort: 5005
            },
            executor: async (options) => {
                const sourceId = options['sourceId']
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const prefix = options['prefix'] || null;

                await Commands.showCurrentTransaction(sourceId, storeIp, storePort, prefix)

                process.exit(0)
            }
        },
        {
            id: "showCommit",
            verbs: ["show", "commit", "!commitSha"],
            options: {
                storeIp: "localhost",
                storePort: 5005
            },
            executor: async (options) => {
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const commitSha = options['commitSha']

                await Commands.showCommit(storeIp, storePort, commitSha)

                process.exit(0)
            }
        },
        {
            id: "lsDirectoryStructure",
            verbs: ["ls", "!directoryDescriptorSha", "?prefix"],
            options: {
                storeIp: "localhost",
                storePort: 5005
            },
            executor: async (options) => {
                const directoryDescriptorSha = options['directoryDescriptorSha']
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const prefix = options['prefix'] || null;

                await Commands.lsDirectoryStructure(storeIp, storePort, directoryDescriptorSha, prefix)

                process.exit(0)
            }
        },
        {
            id: "push",
            verbs: ["push"],
            options: {
                sourceId: defaultSourceId,
                storeIp: "localhost",
                storePort: 5005,
                pushedDirectory: '.'
            },
            executor: async (options) => {
                const sourceId = options['sourceId']
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const pushedDirectory = fsPath.resolve(options['pushedDirectory'])

                await Commands.push(sourceId, pushedDirectory, storeIp, storePort)

                process.exit(0)
            }
        },
        {
            id: "store",
            verbs: ["store"],
            options: {
                storePort: 5005,
                storeDirectory: '.'
            },
            executor: async (options) => {
                const port = options['storePort']
                const directory = fsPath.resolve(options['storeDirectory'])

                await Commands.store(directory, port)
            }
        }
    ])

    function parseAndProcess(args: string[]) {
        let parsed = parseArgs(args, defaultParameters)
        log(`verbs: ${parsed.verbs.join()}    params: ${JSON.stringify(parsed.parameters)}`)
        let processed = cmdManager.process(parsed.verbs, parsed.parameters)
        if (processed)
            log(`command for '${args.join()}': ${processed.id} processed.options: ${JSON.stringify(processed.options)}`)
        else
            log(`no command for '${args.join()}'`)
        return processed
    }

    function shouldBeNull(args) {
        let processed = parseAndProcess(args)
        if (processed)
            log.err(`SHOULD BE NULL : ${args.join()}`);
    }

    function shouldNotBeNull(args) {
        let processed = parseAndProcess(args)
        if (processed == null)
            log.err(`SHOULD NOT BE NULL : ${args.join()} => ${processed.id} / ${JSON.stringify(processed.options)}`);
    }

    //shouldBeNull(["--toto", "-host", "ks387039.kimsufi.com", "push", "famal"])
    //shouldNotBeNull(["push"])

    let processed = parseAndProcess(args)
    if (processed == null) {
        cmdManager.showHelp();
    }
    else {
        try {
            cmdManager.execute(processed);
        }
        catch (error) {
            console.log(`[ERROR] ${error}`)
        }
    }
}

interface CommandSpec {
    id: string;
    verbs: string[];
    options: { [key: string]: any };
    executor: (options: { [key: string]: any }) => void;
}

class CommandManager {
    constructor(private commandSpecs: CommandSpec[]) {
    }

    showHelp() {
        console.log('commands help :')
        console.log()
        console.log('you can put verbs/nouns, "-name value" to set an option and "--name" to put an option to true')
        console.log()
        for (let k in this.commandSpecs) {
            let spec = this.commandSpecs[k]
            console.log(spec.verbs.map((v) => {
                if ('?' == v.charAt(0))
                    return `[${v.slice(1).toUpperCase()}]`
                else if ('!' == v.charAt(0))
                    return v.slice(1).toUpperCase()
                else
                    return v
            }).join(' '))
            if (spec.options) {
                for (let k in spec.options)
                    if (spec.options[k])
                        console.log(`  -${k}: by default ${JSON.stringify(spec.options[k])}`)
                    else
                        console.log(`  -${k}`)
            }
            console.log();
        }
    }

    execute(processed) {
        let spec = this.commandSpecs.find((s) => s.id == processed.id)
        if (spec == null) {
            log.err(`cannot find command ${processed.id}`)
            return false
        }

        spec.executor(processed.options)

        return true
    }

    process(verbs: string[], parameters: { [key: string]: any }): { id: string; options: { [key: string]: any } } {
        for (let i = 0; i < this.commandSpecs.length; i++) {
            let spec = this.commandSpecs[i]

            let options = {}

            if (spec.options)
                for (let k in spec.options)
                    if (spec.options[k])
                        options[k] = spec.options[k]

            if (parameters)
                for (let k in parameters)
                    options[k] = parameters[k]

            // satisfaction des verbes
            let ok = true
            let j = 0
            for (; j < spec.verbs.length; j++) {
                let specVerb = spec.verbs[j]

                if ('?' == specVerb.charAt(0)) {
                    // optional option
                    if (j >= verbs.length)
                        break

                    options[specVerb.slice(1)] = verbs[j]
                }
                else if ('!' == specVerb.charAt(0)) {
                    // required options
                    if (j >= verbs.length) {
                        ok = false
                        break
                    }

                    options[specVerb.slice(1)] = verbs[j]
                }
                else {
                    // pure verbs
                    if (j >= verbs.length || verbs[j] != specVerb) {
                        ok = false
                        break
                    }
                }
            }
            if (ok && j < verbs.length) {
                ok = false
                break
            }

            if (ok)
                return {
                    id: spec.id,
                    options: options
                }
        }

        return null
    }
}

run();