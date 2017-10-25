import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as FS from 'fs'
import * as Stream from 'stream'
import * as Net from 'net'
import * as Serialization from './serialisation'
import { ShaCache } from './ShaCache'
import { HexaBackupStore } from './HexaBackupStore'
import * as Model from './Model'
import * as UploadTransferModel from './UploadTransferModel'
import * as Socket2Message from './Socket2Message'
import * as DirectoryLister from './directory-lister'

const log = require('./Logger')('Tests')
log.conf('dbg', true)

let lister = new DirectoryLister.DirectoryLister('c:\\', [])

lister.on('end', () => {
    log(`finished !`)
})

lister.on('data', (file: UploadTransferModel.FileInfo) => {
    //log(`${file.name}`)
})