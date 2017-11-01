import fs = require('fs');
import fsPath = require('path');
import * as FsTools from './FsTools';
import * as FS from 'fs'
import * as Stream from 'stream'
import * as Net from 'net'
import * as Serialization from './serialisation'
import * as ShaCache from './ShaCache'
import * as Model from './Model'
import * as UploadTransferModel from './UploadTransferModel'
import * as UploadTransferClient from './UploadTransferClient'
import * as Socket2Message from './Socket2Message'
import * as DirectoryLister from './directory-lister'
import * as ShaProcessor from './sha-processor'
import Log from './log'

const log = Log('Tests')
log.conf('dbg', false)

const directory = `d:\\documents`
const cacheDirectory = `d:\\tmp\\hb-cache-test`

function test1() {
    let lister = new DirectoryLister.DirectoryLister(directory)
    lister.on('end', () => log(`finished !`))
    lister.on('data', (file: UploadTransferModel.FileInfo) => { })

    /*let shaProcessor = new ShaProcessor.ShaProcessor(new ShaCache.ShaCache(cacheDirectory))
    lister.pipe(shaProcessor)
    shaProcessor.on('end', () => log(`finished sha processing!`))
    shaProcessor.on('data', (file: UploadTransferModel.FileInfo) => { })*/
}


function testStreamStack1() {
    let lister = new DirectoryLister.DirectoryLister(fsPath.normalize(directory))
    let shaProcessor = new ShaProcessor.ShaProcessor(new ShaCache.ShaCache(cacheDirectory))
    lister.pipe(shaProcessor)

    shaProcessor.on('data', data => log(`chunk ${JSON.stringify(data)}`))
    shaProcessor.on('end', () => log(`end stream stack`))
}

function testStream() {
    let source = fs.createReadStream('d:\\tmp\\20170611_152531.mp4', {
        //let source = fs.createReadStream('d:\\tmp\\ac_queries.txt', {
        flags: 'r',
        encoding: null,
        //autoClose: true
    })

    let dest = fs.createWriteStream('d:\\tmp\\___yiuiu', {
        flags: 'w',
        encoding: null
    })

    let start = new Date().getTime()

    log('starting')
    source.on('end', () => {
        let duration = ((new Date().getTime()) - start) / 1000
        log(`finished in ${duration.toFixed(3)} seconds`)
    })
    source.pipe(dest)
}

testStream()