import fs = require('fs');

export async function lstat(path: string) {
    return new Promise<fs.Stats>((resolve, reject) => {
        fs.lstat(path, (err, stats) => {
            if (err) {
                console.log(`error lstat on ${path} : err`);
                reject(err);
            }
            else
                resolve(stats);
        });
    });
}

export async function openFile(fileName: string, flags: string) {
    return new Promise<number>((resolve, reject) => {
        fs.open(fileName, flags, (err, fd) => {
            if (err)
                reject(err);
            else
                resolve(fd);
        });
    });
}

export async function readFile(fd: number, offset: number, length: number) {
    return new Promise<Buffer>((resolve, reject) => {
        let buffer = new Buffer(length);

        fs.read(fd, buffer, 0, length, offset, (err, bytesRead, buffer) => {
            if (err || bytesRead != length)
                reject(`error reading file`);
            else
                resolve(buffer);
        });
    });
}

export async function writeFile(fd: number, data: string) {
    return new Promise<number>((resolve, reject) => {
        fs.write(fd, data, 0, 'utf8', (err, written, buffer) => {
            if (err)
                reject(err);
            else
                resolve(written);
        });
    });
}

export async function closeFile(fd: number) {
    return new Promise<void>((resolve, reject) => {
        fs.close(fd, (err) => {
            if (err)
                reject(err);
            else
                resolve();
        })
    });
}

export async function readDir(path: string): Promise<string[]> {
    return new Promise<string[]>((resolve, reject) => {
        fs.readdir(path, (err, files) => {
            if (err)
                reject(`error reading directory ${path}`);
            else
                resolve(files);
        });
    });
}