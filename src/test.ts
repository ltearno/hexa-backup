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

const log = require('./Logger')('Tests')
log.conf('dbg', false)

const directory = `d:\\documents\\repos`
const ignoredDirs = ['.hb-cache', '.hb-object', '.hb-refs', '.metadata', '.settings', '.idea', 'target', 'node_modules', 'gwt-unitCache', '.ntvs_analysis.dat', '.gradle', 'student_pictures', 'logs']
const cacheDirectory = `d:\\tmp\\hb-cache-test`

function test1() {
    let lister = new DirectoryLister.DirectoryLister(directory, ignoredDirs)
    //lister.on('end', () => log(`finished !`))
    //lister.on('data', (file: UploadTransferModel.FileInfo) => {})

    let shaProcessor = new ShaProcessor.ShaProcessor(new ShaCache.ShaCache(cacheDirectory))
    lister.pipe(shaProcessor)
    shaProcessor.on('end', () => log(`finished sha processing!`))
    shaProcessor.on('data', (file: UploadTransferModel.FileInfo) => { })
}


function testStreamStack1() {
    let lister = new DirectoryLister.DirectoryLister(directory, ignoredDirs)
    let shaProcessor = new ShaProcessor.ShaProcessor(new ShaCache.ShaCache(cacheDirectory))
    lister.pipe(shaProcessor)

    shaProcessor.on('data', data => log(`chunk ${JSON.stringify(data)}`))
    shaProcessor.on('end', () => log(`end stream stack`))
}

