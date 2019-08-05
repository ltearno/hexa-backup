const fs = require('fs')

const mm = require('music-metadata')

let buffer = fs.readFileSync('/home/arnaud/Téléchargements/teueyetuyte')

//The Roots - The Tipping Point - 06 - Web.mp3
mm.parseBuffer(buffer, "audio/mp3")
    .then(metadata => {
        console.log(`metadata: ${JSON.stringify(metadata)}`, metadata)
    })