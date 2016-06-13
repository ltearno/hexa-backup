const log = require('./Logger')('WorkPool');

export class WorkPool {
    private waitingQueue: any[] = []
    private workInProgress = null
    private finishWaiters = []
    private queuedAdders = []

    constructor(private worker: any, private maxQueueLength) {
    }

    addWork(workItem): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            log.dbg(`add work`)
            this.waitingQueue.push(workItem)
            this.startWork()

            if (this.waitingQueue.length < this.maxQueueLength)
                resolve()
            else
                this.queuedAdders.push(resolve)
        })
    }

    emptied() {
        return new Promise<void>((resolve, reject) => {
            log.dbg(`emptied?`)
            if (this.isCompletelyEmpty()) {
                resolve()
            }
            else {
                this.finishWaiters.push(resolve)
                this.startWork()
            }
        })
    }

    private isCompletelyEmpty = () => ((!this.workInProgress) && this.waitingQueue.length == 0)

    private startWork() {
        if (this.workInProgress || this.waitingQueue.length == 0)
            return

        let batch: any[]

        const max = 50
        if (this.waitingQueue.length < max) {
            batch = this.waitingQueue
            this.waitingQueue = []
        }
        else {
            batch = this.waitingQueue.slice(0, max)
            this.waitingQueue = this.waitingQueue.slice(max)
        }

        log.dbg(`start work of ${batch.length} items`)
        this.workInProgress = this.worker(batch).then(() => {
            this.workInProgress = null

            log.dbg(`finished work of ${batch.length} items`)

            this.signalQueuedAdders()

            if (this.waitingQueue.length == 0)
                this.signalEndWaiters()
            else
                this.startWork()
        }).catch((error) => {
            this.workInProgress = null

            log.err(error)
            log.dbg(`finished with ERROR (${error}) work of ${batch.length} items`)

            if (this.waitingQueue.length == 0)
                this.signalEndWaiters()
            else
                this.startWork()
        })
    }

    private signalQueuedAdders() {
        while (this.queuedAdders.length > 0)
            this.queuedAdders.shift()()
    }

    private signalEndWaiters() {
        log.dbg(`signal end`)
        let len = this.finishWaiters.length
        while (len-- > 0)
            this.finishWaiters.shift()()
    }
}