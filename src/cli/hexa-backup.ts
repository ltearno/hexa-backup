import os = require('os')
import fsPath = require('path')

import { FsTools, LoggerBuilder } from '@ltearno/hexa-js'
import * as Commands from '../Commands'

const log = LoggerBuilder.buildLogger('hexa-backup')
log.conf('dbg', false)

function optionsWith(model) {
    for (let k in model)
        this[k] = model[k]
    return this
}

function optionsWithVerbose() {
    this['verbose'] = false

    return this
}

function optionsWithStore() {
    this['storeIp'] = "localhost"
    this['storePort'] = 5005
    this['storeToken'] = null
    this['storeInsecure'] = false

    return this
}

function optionsWithDatabase() {
    this['database'] = 'postgres'
    this['databaseHost'] = "localhost"
    this['databasePort'] = 5432
    this['databaseUser'] = 'postgres'
    this['databasePassword'] = "hexa-backup"

    return this
}

function optionsWithNoDatabase() {
    this['database'] = null
    this['databaseHost'] = null
    this['databasePort'] = 5432
    this['databaseUser'] = null
    this['databasePassword'] = null

    return this
}

function OptionsConstructor() {
}

function addToPrototype(f: Function) {
    let name = f.name
    name = name.substr('options'.length)
    name = name.substr(0, 1).toLowerCase() + name.substr(1)
    Object.defineProperty(
        OptionsConstructor.prototype,
        name,
        {
            enumerable: false,
            get: () => f
        })
}

addToPrototype(optionsWith)
addToPrototype(optionsWithVerbose)
addToPrototype(optionsWithStore)
addToPrototype(optionsWithDatabase)
addToPrototype(optionsWithNoDatabase)

interface Options {
    with<T, M>(this: T, model: M): T & M
    withVerbose<T>(this: T): T & { verbose: boolean }
    withStore<T>(this: T): T & { storeIp: string; storePort: number; storeToken: string; storeInsecure: boolean }
    withDatabase<T>(this: T): T & { database: string; databaseHost: string; databasePort: number; databaseUser: string; databasePassword: string }
}

function options(): Options {
    return new OptionsConstructor()
}

function getVerboseParam(options: { verbose: boolean }) {
    return !!options['verbose']
}

function getSourceIdParam(options: { sourceId: string }) {
    return options['sourceId'] || null
}

function getStoreParams(options: { storeIp: string; storePort: number; storeToken: string; storeInsecure: boolean }) {
    return {
        host: options['storeIp'],
        port: options['storePort'],
        token: options['storeToken'],
        insecure: !!options['insecure']
    }
}

function getDatabaseParams(options: { database: string; databaseHost: string; databasePort: number; databaseUser: string; databasePassword: string }) {
    if (!options.database || !options.databaseHost || !options.databasePort || !options.databaseUser || !options.databasePassword)
        return null

    return {
        database: options['database'],
        host: options['databaseHost'],
        port: options['databasePort'],
        user: options['databaseUser'],
        password: options['databasePassword']
    }
}

// i only care about people sniffing the local network, not people making man in the middle attacks...
// but that's is bad, for sure
let hacky: any = process.env
hacky["NODE_TLS_REJECT_UNAUTHORIZED"] = 0

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

    const commandSpecs: CommandSpec<any>[] = []

    commandSpecs.push({
        id: "refs",
        verbs: ["refs"],
        options: options()
            .withVerbose()
            .withStore(),
        executor: async (options) => {
            const storeParams = getStoreParams(options)
            const verbose = getVerboseParam(options)

            await Commands.refs(storeParams, verbose)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "sources",
        verbs: ["sources"],
        options: options()
            .withVerbose()
            .withStore(),
        executor: async (options) => {
            const storeParams = getStoreParams(options)
            const verbose = getVerboseParam(options)

            await Commands.sources(storeParams, verbose)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "stats",
        verbs: ["stats"],
        options: options()
            .withStore(),
        executor: async (options) => {
            const storeParams = getStoreParams(options)

            await Commands.stats(storeParams)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "browse",
        verbs: ["browse"],
        options:
            options()
                .with({ directory: '.' })
                .withVerbose(),
        executor: async (options) => {
            const directory = fsPath.resolve(options['directory'])
            const verbose = getVerboseParam(options)

            await Commands.browse(directory, verbose)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "history",
        verbs: ["history"],
        options: options()
            .with({ sourceId: defaultSourceId })
            .withVerbose()
            .withStore(),
        executor: async (options) => {
            const storeParams = getStoreParams(options)
            const sourceId = getSourceIdParam(options)
            const verbose = getVerboseParam(options)

            await Commands.history(sourceId, storeParams, verbose)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "lsDirectoryStructure",
        verbs: ["ls", "!directoryDescriptorSha"],
        options: options()
            .with({ recursive: false })
            .withStore(),
        executor: async (options) => {
            const directoryDescriptorSha = options['directoryDescriptorSha']
            const storeParams = getStoreParams(options)
            const recursive = !!options['recursive']

            await Commands.lsDirectoryStructure(storeParams, directoryDescriptorSha, recursive)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "normalize",
        verbs: ["normalize"],
        options: options()
            .with({ sourceId: defaultSourceId })
            .withStore(),
        executor: async (options) => {
            const sourceId = options['sourceId']
            const storeParams = getStoreParams(options)

            await Commands.normalize(sourceId, storeParams)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "extract",
        verbs: ["extract", "!directoryDescriptorSha", "?prefix"],
        options: options()
            .with({ destinationDirectory: '.' })
            .withStore(),
        executor: async (options) => {
            const directoryDescriptorSha = options['directoryDescriptorSha']
            const prefix = options['prefix'] || null
            const storeParams = getStoreParams(options)
            const destinationDirectory = options['destinationDirectory']

            await Commands.extract(storeParams, directoryDescriptorSha, prefix, destinationDirectory)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "extractSha",
        verbs: ["extractSha", "!sha", "?file"],
        options: options()
            .withStore()
            .with({ file: 'h.out' }),
        executor: async (options) => {
            const sha = options['sha']
            const file = options['file']
            const storeParams = getStoreParams(options)

            await Commands.extractSha(storeParams, sha, file)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "push",
        verbs: ["push"],
        options: options()
            .with({
                sourceId: defaultSourceId,
                pushedDirectory: '.',
                estimateSize: false
            })
            .withStore(),
        executor: async (options) => {
            const sourceId = options['sourceId']
            const storeParams = getStoreParams(options)
            const pushedDirectory = fsPath.resolve(options['pushedDirectory'])
            const estimateSize = options['estimateSize']

            await Commands.push(sourceId, pushedDirectory, storeParams, estimateSize)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "pushStore",
        verbs: ["pushStore"],
        options: options()
            .with({
                storeDirectory: '.',
                estimateSize: false
            })
            .withStore(),
        executor: async (options) => {
            const storeParams = getStoreParams(options)
            const estimateSize = options['estimateSize']
            const directory = fsPath.resolve(options['storeDirectory'])

            await Commands.pushStore(directory, storeParams, estimateSize)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "pull",
        verbs: ["pull", "?sourceId"],
        options: options()
            .with({
                storeDirectory: '.',
                force: false
            })
            .withStore(),
        executor: async (options) => {
            const directory = fsPath.resolve(options['storeDirectory'])
            const storeParams = getStoreParams(options)
            const sourceId = options['sourceId']
            const force = !!options['force']

            await Commands.pull(directory, sourceId, storeParams, force)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "store",
        verbs: ["store"],
        options: options()
            .with({
                storePort: 5005,
                storeInsecure: false,
                storeDirectory: '.',
            })
            .withDatabase(),
        executor: async (options) => {
            const port = options['storePort']
            const insecure = !!options['storeInsecure']
            const directory = fsPath.resolve(options['storeDirectory'])
            const databaseParams = getDatabaseParams(options)

            await Commands.store(directory, port, insecure, databaseParams)
        }
    })
    commandSpecs.push({
        id: "cp",
        verbs: ["cp", "!destination"],
        options: options()
            .with({
                sourceId: `${os.hostname()}-UPLOAD`,
                pushedDirectory: '.',
                recursive: false
            })
            .withStore(),
        executor: async (options) => {
            const source = options['sourceId']
            let pushedDirectory = fsPath.resolve(options['pushedDirectory'])
            const destination = options['destination']
            const storeParams = getStoreParams(options)
            const recursive = !!options['recursive']

            await Commands.copy(source, pushedDirectory, destination, recursive, storeParams)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "merge",
        verbs: ["merge", "!source", "!destination"],
        options: options()
            .with({
                sourceId: defaultSourceId,
                recursive: false
            })
            .withStore(),
        executor: async (options) => {
            const source = options['source']
            const destination = options['destination']
            const storeParams = getStoreParams(options)
            const recursive = !!options['recursive']

            await Commands.merge(source, destination, recursive, storeParams, false)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "dbpush",
        verbs: ["dbpush"],
        options: options()
            .withStore()
            .withDatabase(),
        executor: async (options) => {
            const storeParams = getStoreParams(options)
            const databaseParams = getDatabaseParams(options)

            await Commands.dbPush(storeParams, databaseParams)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "dbimage",
        verbs: ["dbimage"],
        options: options()
            .withStore()
            .withDatabase(),
        executor: async (options) => {
            const storeParams = getStoreParams(options)
            const databaseParams = getDatabaseParams(options)

            await Commands.dbImage(storeParams, databaseParams)

            process.exit(0)
        }
    })
    commandSpecs.push({
        id: "exifextract",
        verbs: ["exifextract"],
        options: options()
            .with({ sourceId: defaultSourceId })
            .withStore()
            .withDatabase(),
        executor: async (options) => {
            const storeParams = getStoreParams(options)
            const databaseParams = getDatabaseParams(options)

            await Commands.exifExtract(storeParams, databaseParams)

            process.exit(0)
        }
    })

    let cmdManager = new CommandManager(commandSpecs)

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

    let processed = parseAndProcess(args)
    if (processed == null) {
        cmdManager.showHelp()
    }
    else {
        try {
            if (processed.options['debug'])
                log.conf('dbg', true)
            log.dbg('== executing in DEBUG mode ==')

            cmdManager.execute(processed);
        }
        catch (error) {
            console.log(`[ERROR] ${error}`)
        }
    }
}

interface CommandSpec<OPTIONS> {
    id: string;
    verbs: string[];
    options: OPTIONS;
    executor: (options: OPTIONS) => void;
}

class CommandManager {
    constructor(private commandSpecs: CommandSpec<any>[]) {
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
                let inorder = []
                for (let k in spec.options)
                    inorder.push(k)
                inorder = inorder.sort()

                for (let k of inorder)
                    if (spec.options[k] !== undefined)
                        console.log(`  -${k.padEnd(25, " ")} ${typeof (spec.options[k] || '')} (default ${spec.options[k] === null ? '""' : JSON.stringify(spec.options[k])})`)
                    else
                        console.log(`  -${k}`)
            }
            console.log()
        }
        console.log()
        console.log(`list of available verbs :`)

        let pad = 18
        let cs = 5
        let s = '  '
        for (let i = 0; i < this.commandSpecs.length; i++) {
            s += this.commandSpecs[i].verbs[0].padEnd(pad)

            if ((i + 1) % cs == 0 || (i == this.commandSpecs.length - 1)) {
                console.log(s)
                s = '  '
            }
        }
        console.log(`you can use "--debug" to activate debug traces`)
    }

    execute(processed) {
        let spec = this.commandSpecs.find((s) => s.id == processed.id)
        if (spec == null) {
            log.err(`cannot find command ${processed.id}`)
            return false
        }

        if (processed.options['debug'])
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