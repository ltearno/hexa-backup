const fs = require('fs')

const mm = require('music-metadata')

//let buffer = fs.readFileSync('/home/arnaud/Téléchargements/teueyetuyte')
//
let buffer = fs.readFileSync('/media/bigone/hexa-backup/.hb-object/2c/2cf06da869e6d5e0560773466cc9ebed7d83621cebf5f5e0f8ff6fe54e902e11')

//The Roots - The Tipping Point - 06 - Web.mp3
mm.parseBuffer(buffer, "audio/mp3")
    .then(metadata => {
        console.log(`metadata: ${JSON.stringify(metadata)}`, metadata)
    })