import crypto = require('crypto');
import fs = require('fs');
import * as FsTools from './FsTools';
import * as Stream from 'stream'
import Log from './log'

const log = Log('HashTools')

export const EMPTY_PAYLOAD_SHA = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

export function hashString(value: string) {
    if (value === "")
        return EMPTY_PAYLOAD_SHA;

    let hash = crypto.createHash('sha256');
    hash.update(value);
    return hash.digest('hex');
}

export async function hashFile(fileName: string): Promise<string> {
    try {
        let stat = await FsTools.lstat(fileName);
        if (stat.size == 0)
            return EMPTY_PAYLOAD_SHA

        log.dbg(`hashing ${fileName}`)

        let input = fs.createReadStream(fileName)

        let sha = await this.hashStream(input)

        log.dbg(`hashing done ${fileName} => ${sha}`)

        return sha
    }
    catch (error) {
        log.err(`error ${fileName}`);
        throw `error hashing ${fileName}`
    }
}

export function hashStream(input: Stream.Readable): Promise<string> {
    return new Promise<string>((resolve, reject) => {
        let hash = crypto.createHash('sha256');

        input.on('data', chunk => {
            hash.update(chunk);
        }).on('end', () => {
            resolve(hash.digest('hex'));
        }).on('error', (err) => {
            reject(err);
        });
    })
}