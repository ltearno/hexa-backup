import { HexaBackupReader } from './HexaBackupReader';
import { HexaBackupStore } from './HexaBackupStore';

import fs = require('fs');

interface YoupiService {
    sayHello(message: string, object: any, buffer: Buffer): Promise<string>;
}

class GrowingBuffer {
    private buffer: Buffer;

    constructor() {
        this.buffer = Buffer.alloc(100);
    }

    extract(size: number) {
        let result = Buffer.alloc(size);
        this.buffer.copy(result, 0, 0, size);
        return result;
    }

    writeByte(offset: number, byte: number): number {
        this.checkEnoughBuffer(offset + 1);
        this.buffer.writeInt8(byte, offset);
        return 1;
    }

    writeUInt32(offset: number, value: number): number {
        this.checkEnoughBuffer(offset + 4);
        this.buffer.writeUInt32LE(value, offset);
        return 4;
    }

    writeBuffer(offset: number, buffer: Buffer): number {
        this.checkEnoughBuffer(offset + 4 + buffer.length);
        this.writeUInt32(offset, buffer.length);
        buffer.copy(this.buffer, offset + 4, 0, buffer.length);
        return 4 + buffer.length;
    }

    writeAny(offset: number, value: any): number {
        let payload = JSON.stringify(value);
        let buffer = new Buffer(payload, 'utf8');

        return this.writeBuffer(offset, buffer);
    }

    private checkEnoughBuffer(size: number) {
        if (this.buffer.length < size) {
            console.log('reallocating the buffer');
            let newBuffer = Buffer.alloc(size * 2);
            this.buffer.copy(newBuffer, 0, 0, this.buffer.length);
            this.buffer = newBuffer;
        }
    }
}

const TYPE_BUFFER = 0;
const TYPE_ANY = 1;

function serializeToBuffer(args: any[]) {
    console.log(`serializing ${args.length} arguments`);

    let currentOffset = 0;
    let buffer: GrowingBuffer = new GrowingBuffer();
    currentOffset += buffer.writeByte(currentOffset, args.length);

    for (let p in args) {
        let arg = args[p];

        if (arg instanceof Buffer) {
            currentOffset += buffer.writeByte(currentOffset, TYPE_BUFFER);
            currentOffset += buffer.writeBuffer(currentOffset, arg);
        }
        else {
            currentOffset += buffer.writeByte(currentOffset, TYPE_ANY);
            currentOffset += buffer.writeAny(currentOffset, arg);
        }
    }

    let result = buffer.extract(currentOffset);

    console.log(`payload serialized to ${result.length} bytes`);

    return result;
}

function deserializeFromBuffer(buffer: Buffer): any[] {
    let currentOffset = 0;

    let nbItems = buffer.readUInt8(currentOffset);
    currentOffset += 1;

    let result = new Array(nbItems);
    for (let i = 0; i < nbItems; i++) {
        let itemType = buffer.readUInt8(currentOffset);
        currentOffset += 1;

        let chunkSize = buffer.readUInt32LE(currentOffset);
        currentOffset += 4;

        let param = null;

        if (itemType == TYPE_BUFFER) {
            param = buffer.slice(currentOffset, currentOffset + chunkSize);
            currentOffset += chunkSize;
        }
        else if (itemType == TYPE_ANY) {
            param = JSON.parse(buffer.slice(currentOffset, currentOffset + chunkSize).toString('utf8'));

            currentOffset += chunkSize;
        }

        result[i] = param;
    }

    return result;
}

let callInfos: { [key: string]: { resolver; rejecter; } } = {};

function createProxy(socket) {
    let handler = {
        get(target, propKey, receiver) {
            return function (...args) {
                return new Promise((resolve, reject) => {
                    args = args.slice();

                    args.unshift(propKey);

                    let callId = '_id_' + Date.now();
                    args.unshift(callId);

                    let payload = serializeToBuffer(args);

                    callInfos[callId] = {
                        resolver: resolve,
                        rejecter: reject
                    };

                    socket.send(payload);
                });
            };
        }
    };
    return new Proxy({}, handler);
}

async function run() {
    let source = ["salut poto", { tat: 'titi' }, new Buffer([1, 5, 7, 8])];
    let sourcePayload = serializeToBuffer(source);
    console.log(`sourcePayload : ${sourcePayload.length}`);

    let received = deserializeFromBuffer(sourcePayload);
    console.log(`received ${received.length} arguments ${JSON.stringify(received)}`);

    var engine = require('engine.io');
    var server = engine.listen(5005);

    let serviceImpl: YoupiService = {
        sayHello: async function (message: string, object: any, buffer?: Buffer): Promise<string> {
            throw "Grossiere erreur !";

            //return `bonjour, ton message '${message}', ton object : '${JSON.stringify(object)}' et ton buffer fait ${buffer.length} octets`;
        }
    };

    server.on('connection', function (socket) {
        socket.on('message', function (...args) {
            if (args == null || args.length != 1) {
                console.log(`received corrupted message !`);
                return;
            }

            let received = deserializeFromBuffer(args[0]);

            let callId = received[0];
            received.shift();
            let method = received[0];
            received.shift();

            console.log(`received message callId:${callId} method:${method}`);
            for (let i = 0; i < received.length; i++) {
                console.log(` -param ${i} : ${JSON.stringify(received[i])}`);
            }
            console.log('calling implementation...');

            let promise: Promise<any> = serviceImpl[method].apply(serviceImpl, received);
            promise.then((value) => {
                console.log(`implementation returned ${value}`);

                let result = [callId, null, value];
                let resultSerialized = serializeToBuffer(result);
                socket.send(resultSerialized);
            }).catch((err) => {
                console.log(`implementation exception ${err}`);

                let result = [callId, err, null];
                let resultSerialized = serializeToBuffer(result);
                socket.send(resultSerialized);
            });

        });
    });

    var socket = require('engine.io-client')('ws://localhost:5005');
    socket.on('open', async function () {
        socket.on('message', function (data) {
            let response = deserializeFromBuffer(data);
            let callId = response[0];
            let err = response[1];
            let returnValue = response[1];

            let callInfo = callInfos[callId];
            delete callInfos[callId];

            console.log(`client received ${JSON.stringify(err)} ${JSON.stringify(response)}`);

            if (err)
                callInfo.rejecter(err);
            else
                callInfo.resolver(response);
        });
        socket.on('close', function () {
            console.log('client connection closed');
        });

        let proxy: YoupiService = createProxy(socket);

        try {
            let result = await proxy.sayHello("salut poto", { tat: 'titi' }, new Buffer([1, 5, 7, 8]));
            console.log(`reply: ${result}`);
        }
        catch (error) {
            console.log(`ERROR! ${error}`);
        }


    });
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

run();