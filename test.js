const mm = require('music-metadata')

mm.parseFile(`/home/arnaud/Téléchargements/The Roots - The Tipping Point - 06 - Web.mp3`)
    .then(metadata => {
        console.log(`metadata: ${JSON.stringify(metadata)}`, metadata)
    })