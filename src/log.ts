const ansiEscapes = require('ansi-escapes')
const ansiStyles = require('ansi-styles')

class Term {
    private stack = []

    saveCursorPosition() {
        this.stack.push(ansiEscapes.cursorSavePosition)
        return this
    }

    restoreCursorPosition() {
        this.stack.push(ansiEscapes.cursorRestorePosition)
        return this
    }

    up(count = 1) {
        this.stack.push(ansiEscapes.cursorUp(count))
        return this
    }

    down(count = 1) {
        this.stack.push(ansiEscapes.cursorDown(count))
        return this
    }

    left(count = 1) {
        while (count--)
            this.stack.push(ansiEscapes.cursorLeft)
        return this
    }

    scrollUp(count = 1) {
        while (count--)
            this.stack.push(ansiEscapes.scrollUp)
        return this
    }

    scrollDown(count = 1) {
        while (count--)
            this.stack.push(ansiEscapes.scrollDown)
        return this
    }

    eraseLine() {
        this.stack.push(ansiEscapes.eraseLine)
        return this
    }

    eraseEndLine() {
        this.stack.push(ansiEscapes.eraseEndLine)
        return this
    }

    eraseDown() {
        this.stack.push(ansiEscapes.eraseDown)
        return this
    }

    str(str) {
        this.stack.push(str)
        return this
    }

    if(condition: boolean, actionner: (term: Term) => void) {
        condition && actionner(this)
        return this
    }

    then(actionner: (term: Term) => void) {
        actionner(this)
        return this
    }

    print() {
        process.stdout.write(new Buffer(this.stack.join(''), 'utf8'))
    }
}

let term = () => new Term()

let nbStatusLines = 3
let lastStatusLines = []
let statusShown = false

function updateStatus(lines: string[]) {
    term()
        .if(lines && statusShown && nbStatusLines != lines.length, t => {
            if (lines.length > nbStatusLines)
                t.scrollUp(lines.length - nbStatusLines)
            else
                t.scrollDown(nbStatusLines - lines.length)
        })
        .if(!statusShown, t => t.scrollUp(lines.length - 1))
        .up(lines.length - 1)
        .left(1000)
        .then(t => printStatus(t, lines))
        .print()
    nbStatusLines = lines.length
    statusShown = true
}

function printStatus(term, lines: string[]) {
    lastStatusLines = lines
    let nb = lines ? lines.length : nbStatusLines
    for (let i = 0; i < nb; i++) {
        if (i)
            term.str('\n')
        term.str(lines ? lines[i] : '')
        term.eraseEndLine()
    }
}

function hideStatus() {
    if (!statusShown)
        return
    statusShown = false

    term()
        .scrollDown(nbStatusLines - 1)
        .left(1000)
        .eraseEndLine()
        .print()
}

function logMessage(text) {
    if (statusShown) {
        term()
            .scrollUp()
            .up(nbStatusLines - 1)
            .left(1000)
            .then(t => printStatus(t, lastStatusLines))
            .up(nbStatusLines)
            .left(1000)
            .str(text)
            .eraseEndLine()
            .down(nbStatusLines)
            .print()
    }
    else {
        console.log(text)
    }
    return
}

class Logger {
    private static config = {
        'log': true,
        'dbg': false,
        'err': true
    };

    private id: string

    private statusInterval: NodeJS.Timer = null
    private statusCallback: () => string[] = null
    private lastStatus: string[] = null

    constructor(id: string) {
        this.id = ('                  ' + id).slice(-17)
    }

    log(message) { this.output('log', message) }
    dbg(message) { this.output('dbg', message) }
    err(message) { this.output('err', message) }
    
    conf(level: string, show: boolean) { Logger.config[level] = show }

    output(level: string, message) {
        if (level in Logger.config && Logger.config[level]) {
            let s: string
            if (message == "" || message == undefined || message == null)
                s = ''
            else if (typeof message === "string")
                s = `[${level}  ${this.id}] ${message}`
            else
                s = `[${level}  ${this.id}] ${JSON.stringify(message)}`

            switch (level) {
                case 'err':
                    s = ansiStyles.red.open + s + ansiStyles.red.close
                    break
                case 'dbg':
                    s = ansiStyles.yellow.open + s + ansiStyles.yellow.close
                    break
            }

            hideStatus()
            logMessage(s + '     ')
        }
    }

    setStatus(callback: () => string[]) {
        if (this.statusInterval == null)
            this.statusInterval = setInterval(() => this.updateStatus(), 700)

        this.statusCallback = callback
    }

    private updateStatus() {
        if (this.statusCallback == null)
            return

        this.lastStatus = this.statusCallback()

        updateStatus([
            ``,
            `${ansiStyles.green.open}[= HEXA-BACKUP =]${ansiStyles.green.close}`,
            ``
        ].concat(this.lastStatus))
    }
}

export default function LoggerBuilder(id: string): {
    (message): void

    log(message): void
    dbg(message): void
    err(message): void
    output(level, message): void

    conf(level, show): void

    setStatus(callback: () => string[]): void
} {
    let logger = new Logger(id)
    let loggerFunction: any = message => logger.log(message)

    loggerFunction.log = (message) => logger.log(message)
    loggerFunction.dbg = (message) => logger.dbg(message)
    loggerFunction.err = (message) => logger.err(message)
    loggerFunction.conf = (level, show) => logger.conf(level, show)
    loggerFunction.output = (level, message) => logger.output(level, message)
    loggerFunction.setStatus = (callback) => logger.setStatus(callback)

    return loggerFunction
}