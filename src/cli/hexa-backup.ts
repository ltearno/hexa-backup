const log = require('../Logger')('hexa-backup');
log.conf('dbg', false);

async function run() {
    let args = process.argv.slice(2).join(" ");
    log(args);
}

run();