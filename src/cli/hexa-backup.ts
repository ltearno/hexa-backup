const log = require('../Logger')('hexa-backup');
log.conf('dbg', false);

async function run() {
    let args = process.argv.slice(2);
    log(JSON.stringify(args));

    /*

    (*) : avec sourceId(default=hostname-directoryname) & store_address (default=localhost:5005)
    on peut trouver les valeurs par defaut dans le fichier .hexa-backup.config si présent

    history (*)
    show current [PREFIX] (*)
    show commit COMMIT_SHA (*)
    ls DIRECTORY_DESCRIPTOR_SHA [PREFIX] (*)
    push PUSHED_DIRECTORY(default=.) (*)
    store [STORE_DIRECTORY(default=.)]
    */
}

run();