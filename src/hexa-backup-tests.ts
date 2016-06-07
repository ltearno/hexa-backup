import { HexaBackupReader } from './HexaBackupReader';
import { IHexaBackupStore, HexaBackupStore } from './HexaBackupStore';
import { RPCClient, RPCServer } from './RPC';

const log = require('./Logger')('tests');

interface YoupiService {
    sayHello(message: string, object: any, buffer: Buffer): Promise<string>;
}

async function run() {
    const serverIp = 'ks387039.kimsufi.com';

    log('connecting to remote store...');
    let rpcClient = new RPCClient();
    let connected = await rpcClient.connect(serverIp, 5005);
    if (!connected) {
        log.err('cannot connect to server !');
        return;
    }

    let store = rpcClient.createProxy<IHexaBackupStore>();

    console.log('history of pc-arnaud in store');
    let sourceState = await store.getSourceState('pc-arnaud');
    let commitSha = sourceState.currentCommitSha;
    if (commitSha == null) {
        console.log('empty !');
    }
    while (commitSha != null) {
        let commit = await store.getCommit(commitSha);
        if (commit == null) {
            console.log(`error : commit ${commitSha} not found !`);
            break;
        }

        let directoryDescriptor = await store.getDirectoryDescriptor(commit.directoryDescriptorSha);
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

        console.log(`${commitSha.substring(0, 7)}: ${new Date(commit.commitDate).toDateString()} ${commit.directoryDescriptorSha.substring(0, 7)} ${totalSize} bytes in ${nbFiles} files, ${nbDirectories} dirs`);

        directoryDescriptor.files.forEach((fd) => {
            console.log(`  ${fd.isDirectory ? '<dir>' : ''} ${fd.name} - ${new Date(fd.lastWrite).toDateString()} ${fd.size}`);
        });

        commitSha = commit.parentSha;
    }

    process.exit(0);
}

async function runb() {
    /*let source = ["salut poto", { tat: 'titi' }, new Buffer([1, 5, 7, 8])];
    let sourcePayload = Serialization.serialize(source);
    console.log(`sourcePayload : ${sourcePayload.length}`);

    let received = Serialization.deserialize(sourcePayload);
    console.log(`received ${received.length} arguments ${JSON.stringify(received)}`);*/

    let serviceImpl: YoupiService = {
        sayHello: async function (message: string, object: any, buffer?: Buffer): Promise<string> {
            //throw "Grossiere erreur !";
            return `bonjour, ton message '${message}', ton object : '${JSON.stringify(object)}' et ton buffer fait ${buffer.length} octets`;
        }
    };

    console.log('init server');
    let rpcServer = new RPCServer();
    rpcServer.listen(5005, serviceImpl);

    console.log('yo');

    let rpcClient = new RPCClient();
    let connected = await rpcClient.connect('localhost', 5005);
    if (!connected) {
        console.log('cannot connect to server !');
        return;
    }

    console.log('init client');
    let proxy = rpcClient.createProxy<YoupiService>();
    try {
        let result = await proxy.sayHello("salut poto", { tat: 'titi' }, new Buffer([1, 5, 7, 8]));
        console.log(`reply: ${result}`);
    }
    catch (error) {
        console.log(`ERROR! ${error}`);
    }

    console.log('finito');
}

async function runa() {
    console.log("Test load for Hexa-Backup !");

    let backupedDirectory = `D:\\Tmp\\Conseils d'Annelise pour la prochaine AG`;
    //let backupedDirectory = `D:\\Documents`;

    let reader = new HexaBackupReader(backupedDirectory, 'pc-arnaud');
    //let desc = await reader.readDirectoryState();
    //console.log(`descriptor: ${JSON.stringify(desc)}`);

    let store = new HexaBackupStore(`D:\\Tmp\\HexaBackupStore`);

    await reader.sendSnapshotToStore(store);

    console.log('finish');
}

async function old() {
    /*let fileName = 'package.json'; //'d:\\downloads\\crackstation.txt.gz'
    let hex = await hashFile(fileName);
    console.log(`hash: ${hex}`);

    let dirName = '.';
    let files = await readDir(dirName);
    console.log(`files: ${files.join()}`);

    let shaCache = new ShaCache(`d:\\tmp\\.hb-cache`);
    let shaContent = await shaCache.hashFile('d:\\tmp\\CCF26012016.png');
    shaCache.flushToDisk();
    console.log(`picture sha : ${shaContent}`);*/
}

run();