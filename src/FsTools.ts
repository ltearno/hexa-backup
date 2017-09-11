import fs = require('fs');

export async function fileExists(path: string) {
    return new Promise<boolean>((resolve, reject) => {
        fs.exists(path, (exists) => resolve(exists));
    });
}

export async function lstat(path: string) {
    return new Promise<fs.Stats>((resolve, reject) => {
        fs.lstat(path, (err, stats) => {
            if (err)
                reject(err)
            else
                resolve(stats)
        });
    });
}

export async function stat(path: string) {
    return new Promise<fs.Stats>((resolve, reject) => {
        fs.stat(path, (err, stats) => {
            if (err)
                reject(err)
            else
                resolve(stats)
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

export async function readFileContent(path: string, encoding?: string) {
    return new Promise<string>((resolve, reject) => {
        fs.readFile(path, encoding, (err, data) => {
            if (err)
                reject(err)
            else
                resolve(data)
        })
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

export async function writeFileBuffer(fd: number, offset: number, buffer: Buffer) {
    return new Promise<number>((resolve, reject) => {
        fs.write(fd, buffer, 0, buffer.byteLength, offset, (err, written, buffer) => {
            if (err)
                reject(err)
            else
                resolve(written)
        })
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

export function readDirSync(path: string): string[] {
    return fs.readdirSync(path)
}