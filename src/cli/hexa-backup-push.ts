import { HexaBackupReader } from '../HexaBackupReader';
import { IHexaBackupStore } from '../HexaBackupStore';
import { RPCClient, RPCServer } from '../RPC';

const log = require('../Logger')('hexa-backup-push');
log.conf('dbg', false);

async function run() {
    if (process.argv.length != 2 + 3) {
        log.err(`usage: hexa-backup-push sourceId directory server_ip`);
        return;
    }

    let sourceId = process.argv[2];
    let pushedDirectory = process.argv[3];
    let storeIp = process.argv[4];
    let storePort = 5005;

    log(`source: ${sourceId}`);
    log(`directory: ${pushedDirectory}`);
    log(`server: ${storeIp}:${storePort}`);
    log();

    log('connecting to remote store...');
    let rpcClient = new RPCClient();
    let connected = await rpcClient.connect(storeIp, 5005);
    if (!connected) {
        log.err('cannot connect to server !');
        return;
    }

    log('connected');

    log('preparing directory reader');
    let reader = new HexaBackupReader(pushedDirectory, sourceId);

    let remoteStore = rpcClient.createProxy<IHexaBackupStore>();

    log('sending directory snapshot to remote store');
    await reader.sendSnapshotToStore(remoteStore);

    log('finished');
}

run();