import { HexaBackupReader } from './HexaBackupReader';
import { HexaBackupStore } from './HexaBackupStore';
//import { ClientState, Commit, DirectoryDescriptor, FileDescriptor } from 'Model';

run();

async function run() {
    console.log("Test load for Hexa-Backup !");

    let backupedDirectory = `D:\\Tmp\\Conseils d'Annelise pour la prochaine AG`;
    //let backupedDirectory = `D:\\Documents`;

    let reader = new HexaBackupReader(backupedDirectory, 'pc-arnaud');
    //let desc = await reader.readDirectoryState();
    //console.log(`descriptor: ${JSON.stringify(desc)}`);

    let store = new HexaBackupStore(`D:\\Tmp\\HexaBackupStore`);

    await reader.sendSnapshotToStore(store);

    console.log('history of pc-arnaud in store');
    let sourceState = await store.getSourceState('pc-arnaud');
    let commitSha = sourceState.currentCommitSha;
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