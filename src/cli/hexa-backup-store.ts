import { HexaBackupStore } from '../HexaBackupStore';
import { RPCClient, RPCServer } from '../RPC';

const log = require('../Logger')('hexa-backup-store');
log.conf('dbg', false);

async function run() {
    const port = 5005;

    log('preparing store');
    let store = new HexaBackupStore('.');

    log('server intialisation');
    let rpcServer = new RPCServer();
    rpcServer.listen(port, store);

    log(`ready on port ${port} !`);
}

run();