import * as Net from 'net'
import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
import { RPCClient, RPCServer } from './RPC'
import { HashTools, FsTools, LoggerBuilder } from '@ltearno/hexa-js'
import * as Model from './Model'
import fsPath = require('path')
import * as fs from 'fs'
import * as UploadTransferServer from './UploadTransferServer'
import * as UploadTransferClient from './UploadTransferClient'

const log = LoggerBuilder.buildLogger('Commands')

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

    if (sourceState.currentTransactionId) {
        let emptySha = '                                                                '
        console.log()
        console.log(`current transaction ${sourceState.currentTransactionId}`)
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

        console.log(`fetching ${fileDesc.name}`)

        let destinationFilePath = fsPath.join(destinationDirectory, fileDesc.name)

        if (fileDesc.isDirectory) {
            try {
                fs.mkdirSync(destinationFilePath)
            } catch (error) {
                log("error : " + error)
            }
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

        let lastWriteUnix = parseInt((fileDesc.lastWrite / 1000).toFixed(0))
        fs.utimesSync(destinationFilePath, lastWriteUnix, lastWriteUnix)
    }
}

export async function pushFast(sourceId, pushedDirectory, storeIp, storePort, estimateSize) {
    return new Promise((accept, reject) => {
        console.log(`push options :`)
        console.log(`  directory: ${pushedDirectory}`)
        console.log(`  source: ${sourceId}`)
        console.log(`  server: ${storeIp}:${storePort}`)
        console.log(`  estimateSize: ${estimateSize}`)
        console.log()

        let socket = new Net.Socket()

        socket.on('connect', () => {
            log(`connected to ${storeIp}:${storePort}`)

            let client = new UploadTransferClient.UploadTransferClient(pushedDirectory, sourceId, estimateSize, socket)
            client.start()
        })

        socket.on('close', () => {
            log('connection closed')
            accept()
        })

        socket.on('error', (err) => {
            socket.end()
            reject(err)
        })

        socket.connect(storePort, storeIp)
    })
}

export async function store(directory, port) {
    console.log(`preparing store in ${directory}`);
    let store = new HexaBackupStore(directory);

    console.log('server intialisation');
    let rpcServer = new RPCServer();
    rpcServer.listen(port, store);

    let transferServer = new UploadTransferServer.UploadTransferServer()
    transferServer.listen(port + 1, store)

    console.log(`ready on port ${port} !`);
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