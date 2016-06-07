class Logger {
    private static config = {
        'log': true,
        'dbg': true,
        'err': true
    };

    private id: string;

    constructor(id: string) {
        this.id = ('                  ' + id).slice(-17);
    }

    log(message) {
        this.output('log', message);
    }

    dbg(message) {
        this.output('dbg', message);
    }

    err(message) {
        this.output('err', message);
    }

    conf(level: string, show: boolean) {
        Logger.config[level] = show;
    }

    output(level, message) {
        if (level in Logger.config && Logger.config[level]) {
            if (message == "" || message == undefined || message == null)
                console.log();
            else if (typeof message === "string")
                console.log(`[${level}  ${this.id}] ${message}`);
            else
                console.log(`[${level}  ${this.id}] ${JSON.stringify(message)}`);
        }
    }
}

function LoggerBuilder(id: string): {
    (message): void;

    log(message): void;
    dbg(message): void;
    err(message): void;
    output(level, message): void;

    conf(level, show): void;
} {
    let logger = new Logger(id);

    let loggerFunction: any = function (message) {
        logger.log(message);
    };

    loggerFunction.log = (message) => logger.log(message);
    loggerFunction.dbg = (message) => logger.dbg(message);
    loggerFunction.err = (message) => logger.err(message);
    loggerFunction.conf = (level, show) => logger.conf(level, show);
    loggerFunction.output = (level, message) => logger.output(level, message);

    return loggerFunction;
}

export = LoggerBuilder;