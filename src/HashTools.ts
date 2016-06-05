import crypto = require('crypto');
import fs = require('fs');
import * as FsTools from './FsTools';

export const EMPTY_PAYLOAD_SHA = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

export function hashString(value: string) {
    if (value === "")
        return EMPTY_PAYLOAD_SHA;

    let hash = crypto.createHash('sha256');
    hash.update(value);
    return hash.digest('hex');
}

export async function hashFile(fileName: string): Promise<string> {
    return new Promise<string>(async (resolve, reject) => {
        let hash = crypto.createHash('sha256');

        try {
            let stat = await FsTools.lstat(fileName);
            if (stat.size == 0) {
                resolve(EMPTY_PAYLOAD_SHA);
                return;
            }
        }
        catch (error) {
            console.log(`error reading ${fileName}`);
            reject(`error reading ${fileName}`);
            return;
        }

        let input = fs.createReadStream(fileName);

        input.on('data', chunk => {
            hash.update(chunk);
        }).on('end', () => {
            resolve(hash.digest('hex'));
        }).on('error', () => {
            console.log(`error reading ${fileName}`);
            reject(`error reading ${fileName}`);
        });
    });
}