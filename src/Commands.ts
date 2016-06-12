import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
import { HexaBackupReader } from './HexaBackupReader'
import { RPCClient, RPCServer } from './RPC'
import * as Model from './Model'

const log = require('./Logger')('hexa-backup-commands')

export async function history(sourceId, storeIp, storePort) {
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

    console.log('history of pc-arnaud in store');
    console.log()

    let sourceState = await store.getSourceState(sourceId);

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

    if (directoryDescriptorShaToShow) {
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

export async function lsDirectoryStructure(storeIp, storePort, directoryDescriptorSha, prefix) {
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

    showDirectoryDescriptor(directoryDescriptor)
}

export async function push(sourceId, pushedDirectory, storeIp, storePort) {
    console.log(`push options :`)
    console.log(`  directory: ${pushedDirectory}`);
    console.log(`  source: ${sourceId}`);
    console.log(`  server: ${storeIp}:${storePort}`);
    console.log();

    console.log('connecting to remote store...')
    let remoteStore = null
    try {
        log('connecting to remote store...')
        let rpcClient = new RPCClient()
        let connected = await rpcClient.connect(storeIp, storePort)
        if (!connected)
            throw 'cannot connect to server !'

        log('connected')
        remoteStore = rpcClient.createProxy<IHexaBackupStore>()
    }
    catch (error) {
        console.log(`[ERROR] cannot connect to server : ${error} !`)
        return
    }

    console.log('preparing directory reader');
    let reader = new HexaBackupReader(pushedDirectory, sourceId);

    console.log('sending directory snapshot to remote store');
    await reader.sendSnapshotToStore(remoteStore);

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

function showDirectoryDescriptor(directoryDescriptor: Model.DirectoryDescriptor) {
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
        console.log(`${fd.isDirectory ? '<dir>' : '     '} ${new Date(fd.lastWrite).toDateString()} ${('            ' + (fd.isDirectory ? '' : fd.size)).slice(-12)}    ${fd.contentSha ? fd.contentSha : emptySha}  ${fd.name}`);
    });
}