let Gauge = require('gauge');

const colors = require('colors/safe')

class Logger {
    private static config = {
        'log': true,
        'dbg': true,
        'err': true
    };

    private id: string

    private statusInterval: NodeJS.Timer = null
    private statusCallback: () => { message: string; completed: number; } = null
    private lastStatus: { message: string; completed: number; } = null

    private _gauge: any = null

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

    output(level: string, message) {
        if (level in Logger.config && Logger.config[level]) {
            this.hideGauge()

            let s
            if (message == "" || message == undefined || message == null)
                s = ''
            else if (typeof message === "string")
                s = `[${level}  ${this.id}] ${message}`
            else
                s = `[${level}  ${this.id}] ${JSON.stringify(message)}`

            switch (level) {
                case 'err':
                    s = colors.red(s)
                    break
                case 'dbg':
                    s = colors.yellow(s)
                    break
            }

            console.log(s)
        }
    }

    setStatus(callback: () => { message: string; completed: number; }) {
        if (this.statusInterval == null)
            this.statusInterval = setInterval(() => this.updateStatus(), 1000)

        this.statusCallback = callback
    }

    private updateStatus() {
        if (this.statusCallback == null)
            return

        this.lastStatus = this.statusCallback()

        this.gauge().show(this.lastStatus.message || '(no status)', this.lastStatus.completed)
    }

    private gauge() {
        if (this._gauge == null)
            this._gauge = new Gauge()

        return this._gauge
    }

    private hideGauge() {
        if (this._gauge)
            this._gauge.hide()
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
    loggerFunction.setStatus = (callback) => logger.setStatus(callback);

    return loggerFunction;
}

export = LoggerBuilder;