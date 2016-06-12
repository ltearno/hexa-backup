import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore'
import { HexaBackupReader } from './HexaBackupReader'
import { RPCClient, RPCServer } from './RPC'

const log = require('./Logger')('hexa-backup-commands')

async function connectStore(storeIp, storePort) {
    log('connecting to remote store...')
    let rpcClient = new RPCClient()
    let connected = await rpcClient.connect(storeIp, storePort)
    if (!connected)
        throw 'cannot connect to server !'

    log('connected')
    return rpcClient.createProxy<IHexaBackupStore>()
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
        //remoteStore = await connectStore(storeIp, storePort)
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