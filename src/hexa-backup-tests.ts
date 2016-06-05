import { HexaBackupReader } from './HexaBackupReader';
import { HexaBackupStore } from './HexaBackupStore';
//import { ClientState, Commit, DirectoryDescriptor, FileDescriptor } from 'Model';

run();

async function run() {
    console.log("Test load for Hexa-Backup !");

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

    //let backupedDirectory = `D:\\Tmp\\Conseils d'Annelise pour la prochaine AG`;
    let backupedDirectory = `D:\\Documents`;

    let reader = new HexaBackupReader(backupedDirectory, 'pc-arnaud');
    //let desc = await reader.readDirectoryState();
    //console.log(`descriptor: ${JSON.stringify(desc)}`);

    let store = new HexaBackupStore(`D:\\Tmp\\HexaBackupStore`);

    await reader.sendSnapshotToStore(store);
    console.log('finish');
}