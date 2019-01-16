import os = require('os')
import fsPath = require('path')

import { FsTools, LoggerBuilder } from '@ltearno/hexa-js'
import * as Commands from '../Commands'

const log = LoggerBuilder.buildLogger('hexa-backup')
log.conf('dbg', false)

// i only care about people sniffing the local network, not people making man in the middle attacks...
// but that's is bad, for sure
process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0

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
        let stat = await FsTools.lstat(parametersFileName);
        if (stat != null) {
            let content = await FsTools.readFileContent(parametersFileName, 'utf8')
            defaultParameters = JSON.parse(content) || {}
        }
    }
    catch (error) {
    }

    let defaultSourceId = `${os.hostname()}-${fsPath.basename(process.cwd())}`

    let cmdManager = new CommandManager([
        {
            id: "refs",
            verbs: ["refs"],
            options: {
                storeIp: "localhost",
                storePort: 5005,
                verbose: false,
                insecure: false
            },
            executor: async (options) => {
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const verbose = options['verbose']
                const insecure = !!options['insecure']

                await Commands.refs(storeIp, storePort, verbose, insecure)

                process.exit(0)
            }
        },
        {
            id: "sources",
            verbs: ["sources"],
            options: {
                storeIp: "localhost",
                storePort: 5005,
                verbose: false,
                insecure: false
            },
            executor: async (options) => {
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const verbose = options['verbose']
                const insecure = !!options['insecure']

                await Commands.sources(storeIp, storePort, verbose, insecure)

                process.exit(0)
            }
        },
        {
            id: "stats",
            verbs: ["stats"],
            options: {
                storeIp: "localhost",
                storePort: 5005,
                verbose: false,
                insecure: false
            },
            executor: async (options) => {
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const verbose = options['verbose']
                const insecure = !!options['insecure']

                await Commands.stats(storeIp, storePort, verbose, insecure)

                process.exit(0)
            }
        },
        {
            id: "browse",
            verbs: ["browse"],
            options: {
                directory: '.',
                verbose: false
            },
            executor: async (options) => {
                const directory = fsPath.resolve(options['directory'])
                const verbose = options['verbose']

                await Commands.browse(directory, verbose)

                process.exit(0)
            }
        },
        {
            id: "history",
            verbs: ["history"],
            options: {
                sourceId: defaultSourceId,
                storeIp: "localhost",
                storePort: 5005,
                verbose: false,
                insecure: false
            },
            executor: async (options) => {
                const sourceId = options['sourceId']
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const verbose = options['verbose']
                const insecure = !!options['insecure']

                await Commands.history(sourceId, storeIp, storePort, verbose, insecure)

                process.exit(0)
            }
        },
        {
            id: "lsDirectoryStructure",
            verbs: ["ls", "!directoryDescriptorSha"],
            options: {
                storeIp: "localhost",
                storePort: 5005,
                recursive: false,
                insecure: false
            },
            executor: async (options) => {
                const directoryDescriptorSha = options['directoryDescriptorSha']
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const recursive = options['recursive'] || false
                const insecure = !!options['insecure']

                await Commands.lsDirectoryStructure(storeIp, storePort, directoryDescriptorSha, recursive, insecure)

                process.exit(0)
            }
        },
        {
            id: "normalize",
            verbs: ["normalize"],
            options: {
                sourceId: defaultSourceId,
                storeIp: "localhost",
                storePort: 5005,
                insecure: false
            },
            executor: async (options) => {
                const sourceId = options['sourceId']
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const insecure = !!options['insecure']

                await Commands.normalize(sourceId, storeIp, storePort, false, insecure)

                process.exit(0)
            }
        },
        {
            id: "extract",
            verbs: ["extract", "!directoryDescriptorSha", "?prefix"],
            options: {
                storeIp: "localhost",
                storePort: 5005,
                destinationDirectory: '.',
                insecure: false
            },
            executor: async (options) => {
                const directoryDescriptorSha = options['directoryDescriptorSha']
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const destinationDirectory = options['destinationDirectory']
                const prefix = options['prefix'] || null
                const insecure = !!options['insecure']

                await Commands.extract(storeIp, storePort, directoryDescriptorSha, prefix, destinationDirectory, insecure)

                process.exit(0)
            }
        },
        {
            id: "extractSha",
            verbs: ["extractSha", "!sha", "?file"],
            options: {
                storeIp: "localhost",
                storePort: 5005,
                file: 'h.out',
                insecure: false
            },
            executor: async (options) => {
                const sha = options['sha']
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const file = options['file']
                const insecure = !!options['insecure']

                await Commands.extractSha(storeIp, storePort, sha, file, insecure)

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
                pushedDirectory: '.',
                estimateSize: false,
                insecure: false
            },
            executor: async (options) => {
                const sourceId = options['sourceId']
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const pushedDirectory = fsPath.resolve(options['pushedDirectory'])
                const estimateSize = options['estimateSize']
                const insecure = !!options['insecure']

                await Commands.push(sourceId, pushedDirectory, storeIp, storePort, estimateSize, insecure)

                process.exit(0)
            }
        },
        {
            id: "pushStore",
            verbs: ["pushStore"],
            options: {
                storeIp: "localhost",
                storePort: 5005,
                estimateSize: false,
                storeDirectory: '.',
                insecure: false
            },
            executor: async (options) => {
                const storeIp = options['storeIp']
                const storePort = options['storePort']
                const estimateSize = options['estimateSize']
                const directory = fsPath.resolve(options['storeDirectory'])
                const insecure = !!options['insecure']

                await Commands.pushStore(directory, storeIp, storePort, estimateSize, insecure)

                process.exit(0)
            }
        },
        {
            id: "store",
            verbs: ["store"],
            options: {
                storePort: 5005,
                storeDirectory: '.',
                insecure: false
            },
            executor: async (options) => {
                const port = options['storePort']
                const directory = fsPath.resolve(options['storeDirectory'])
                const insecure = !!options['insecure']

                await Commands.store(directory, port, insecure)
            }
        }
    ])

    function parseAndProcess(args: string[]) {
        let parsed = parseArgs(args, defaultParameters)
        log.dbg(`verbs: ${parsed.verbs.join()}    params: ${JSON.stringify(parsed.parameters)}`)
        let processed = cmdManager.process(parsed.verbs, parsed.parameters)
        if (processed)
            log.dbg(`command for '${args.join()}': ${processed.id} processed.options: ${JSON.stringify(processed.options)}`)
        else
            log(`not understood : '${args.join()}'...`)
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

    let processed = parseAndProcess(args)
    if (processed == null) {
        cmdManager.showHelp();
    }
    else {
        try {
            if ('debug' in processed.options && processed.options['debug'])
                log.conf('dbg', true)
            log.dbg('== executing in DEBUG mode ==')

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
            console.log()
        }
        console.log()
        console.log(`you can use "--debug" to activate debug traces`)
    }

    execute(processed) {
        let spec = this.commandSpecs.find((s) => s.id == processed.id)
        if (spec == null) {
            log.err(`cannot find command ${processed.id}`)
            return false
        }

        if ('debug' in processed.options)
            log.conf('dbg', true)

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