import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
import { HexaBackupReader } from './HexaBackupReader'
import { RPCClient, RPCServer } from './RPC'
import * as HashTools from './HashTools'
import * as FsTools from './FsTools'
import * as Model from './Model'
import fsPath = require('path')
import * as fs from 'fs'

const log = require('./Logger')('Commands')

export async function history(sourceId, storeIp, storePort, verbose) {
    console.log('connecting to remote store...')
    let store: IHexaBackupStore = null
    try {
        log('connecting to remote store...')
        let rpcClient = new RPCClient()
        let connected = await rpcClient.connect(storeIp, storePort)
        if (!connected)
            throw 'cannot connect to server !'

        log('connected')
        store = rpcClient.createProxy<IHexaBackupStore>()
    }
    catch (error) {
        console.log(`[ERROR] cannot connect to server : ${error} !`)
        return
    }

    console.log('history of pc-arnaud in store');
    console.log()

    let sourceState = await store.getSourceState(sourceId);

    if (sourceState == null) {
        console.log(`source state not found !`)
        return
    }

    if (sourceState.currentTransactionId && sourceState.currentTransactionContent) {
        let emptySha = '                                                                '
        console.log()
        console.log(`current transaction ${sourceState.currentTransactionId}`)
        if (verbose) {
            for (let k in sourceState.currentTransactionContent) {
                let fd = sourceState.currentTransactionContent[k]
                console.log(`${fd.isDirectory ? '<dir>' : '     '} ${new Date(fd.lastWrite).toDateString()} ${('            ' + (fd.isDirectory ? '' : fd.size)).slice(-12)}    ${fd.contentSha ? fd.contentSha : emptySha}  ${fd.name}`);
            }
        }
    }

    let directoryDescriptorShaToShow = null
    let commitSha = sourceState.currentCommitSha
    if (commitSha == null)
        console.log('empty !')

    while (commitSha != null) {
        let commit = await store.getCommit(commitSha);
        if (commit == null) {
            console.log(`error : commit ${commitSha} not found !`);
            break;
        }

        console.log(`${new Date(commit.commitDate).toDateString()} commit ${commitSha} desc:${commit.directoryDescriptorSha}`);

        if (directoryDescriptorShaToShow == null)
            directoryDescriptorShaToShow = commit.directoryDescriptorSha

        commitSha = commit.parentSha
    }

    if (verbose && directoryDescriptorShaToShow) {
        console.log()
        console.log(`most recent commit's directory structure (${directoryDescriptorShaToShow}) :`)
        let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorShaToShow)
        showDirectoryDescriptor(directoryDescriptor)
    }
}

export async function showCurrentTransaction(sourceId, storeIp, storePort, prefix) {
}

export async function showCommit(storeIp, storePort, commitSha) {
}

export async function lsDirectoryStructure(storeIp, storePort, directoryDescriptorSha, prefix: string) {
    console.log('connecting to remote store...')
    let store = null
    try {
        log('connecting to remote store...')
        let rpcClient = new RPCClient()
        let connected = await rpcClient.connect(storeIp, storePort)
        if (!connected)
            throw 'cannot connect to server !'

        log('connected')
        store = rpcClient.createProxy<IHexaBackupStore>()
    }
    catch (error) {
        console.log(`[ERROR] cannot connect to server : ${error} !`)
        return
    }

    let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorSha);

    showDirectoryDescriptor(directoryDescriptor, prefix)
}

export async function extract(storeIp, storePort, directoryDescriptorSha, prefix: string, destinationDirectory: string) {
    console.log('connecting to remote store...')
    let store: IHexaBackupStore = null
    try {
        log('connecting to remote store...')
        let rpcClient = new RPCClient()
        let connected = await rpcClient.connect(storeIp, storePort)
        if (!connected)
            throw 'cannot connect to server !'

        log('connected')
        store = rpcClient.createProxy<IHexaBackupStore>()
    }
    catch (error) {
        console.log(`[ERROR] cannot connect to server : ${error} !`)
        return
    }

    console.log('getting directory descriptor...')
    let directoryDescriptor = await store.getDirectoryDescriptor(directoryDescriptorSha);

    showDirectoryDescriptor(directoryDescriptor, prefix)

    destinationDirectory = fsPath.resolve(destinationDirectory)

    console.log(`extracting ${directoryDescriptorSha} to ${destinationDirectory}, prefix='${prefix}'...`)

    for (let k in directoryDescriptor.files) {
        let fileDesc = directoryDescriptor.files[k]

        if (prefix && !fileDesc.name.startsWith(prefix))
            continue

        let destinationFilePath = fsPath.join(destinationDirectory, fileDesc.name)

        if (fileDesc.isDirectory) {
            fs.mkdirSync(destinationFilePath)
        }
        else {
            let fileLength = await store.hasOneShaBytes(fileDesc.contentSha)

            let currentReadPosition = 0
            try {
                let stat = await FsTools.lstat(destinationFilePath)
                currentReadPosition = stat.size
            }
            catch (error) {
            }

            let fd = await FsTools.openFile(destinationFilePath, 'a')

            const maxSize = 1024 * 100
            while (currentReadPosition < fileLength) {
                let size = fileLength - currentReadPosition
                if (size > maxSize)
                    size = maxSize

                let buffer = await store.readShaBytes(fileDesc.contentSha, currentReadPosition, size)

                await FsTools.writeFileBuffer(fd, currentReadPosition, buffer)

                currentReadPosition += size
            }

            await FsTools.closeFile(fd)

            let contentSha = await HashTools.hashFile(destinationFilePath)
            if (contentSha != fileDesc.contentSha) {
                log.err(`extracted file signature is inconsistent : ${contentSha} != ${fileDesc.contentSha}`)
            }

            log(`extracted ${fileDesc.name}`)
        }
    }
}

export async function push(sourceId, pushedDirectory, storeIp: string, storePort, useZip: boolean) {
    console.log(`push options :`)
    console.log(`  directory: ${pushedDirectory}`);
    console.log(`  source: ${sourceId}`);
    console.log(`  server: ${storeIp}:${storePort}`);
    console.log(` use zip: ${useZip}`)
    console.log();
    console.log(`To push locally, prepend 'storeIp' with 'file://'`);
    console.log();

    let store: IHexaBackupStore = null

    const FILE_MARKER = 'file://'
    if (storeIp.startsWith(FILE_MARKER)) {
        let directory = storeIp.substring(FILE_MARKER.length)

        console.log(`preparing local store in ${directory}`)
        store = new HexaBackupStore(directory)
    }
    else {
        console.log('connecting to remote store...')
        try {
            log('connecting to remote store...')
            let rpcClient = new RPCClient()
            let connected = await rpcClient.connect(storeIp, storePort)
            if (!connected)
                throw 'cannot connect to server !'

            log('connected')
            store = rpcClient.createProxy<IHexaBackupStore>()
        }
        catch (error) {
            console.log(`[ERROR] cannot connect to server : ${error} !`)
            return
        }
    }

    console.log('preparing directory reader');
    let reader = new HexaBackupReader(pushedDirectory, sourceId);

    console.log('sending directory snapshot to remote store');
    await reader.sendSnapshotToStore(store, useZip);

    console.log(`finished, directory ${pushedDirectory} pushed`);
}

export async function store(directory, port) {
    console.log(`preparing store in ${directory}`);
    let store = new HexaBackupStore(directory);

    console.log('server intialisation');
    let rpcServer = new RPCServer();
    rpcServer.listen(port, store);

    console.log(`ready on port ${port} !`);
}

async function connectStore(storeIp, storePort) {
    log('connecting to remote store...')
    let rpcClient = new RPCClient()
    let connected = await rpcClient.connect(storeIp, storePort)
    if (!connected)
        throw 'cannot connect to server !'

    log('connected')
    return rpcClient.createProxy<IHexaBackupStore>()
}

function showDirectoryDescriptor(directoryDescriptor: Model.DirectoryDescriptor, prefix?: string) {
    let totalSize = 0;
    let nbFiles = 0;
    let nbDirectories = 0;
    directoryDescriptor.files.forEach((fd) => {
        totalSize += fd.size;
        if (fd.isDirectory)
            nbDirectories++;
        else
            nbFiles++;
    });

    console.log(`${totalSize} bytes in ${nbFiles} files, ${nbDirectories} dirs`);

    let emptySha = '                                                                '

    directoryDescriptor.files.forEach((fd) => {
        if (!prefix || fd.name.startsWith(prefix))
            console.log(`${fd.isDirectory ? '<dir>' : '     '} ${new Date(fd.lastWrite).toDateString()} ${('            ' + (fd.isDirectory ? '' : fd.size)).slice(-12)}    ${fd.contentSha ? fd.contentSha : emptySha} ${fd.name} `);
    });
}