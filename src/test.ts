import * as FS from 'fs'

let readable = FS.createReadStream('D:\\Tmp\\tmp\\MVI_0545.MOV')

readable.on('data', (chunk) => {
    console.log(`Received ${chunk.length} bytes of data.`);
    readable.pause();
    console.log('There will be no additional data for 1 second.');
    setTimeout(() => {
        console.log('Now data will start flowing again.');
        readable.resume();
    }, 1000);
});