const log = require('./Logger')('WorkPool');

export class WorkPool {
    private waitingQueue: any[] = []
    private workInProgress = null
    private finishWaiters = []

    constructor(private worker: any) {
    }

    addWork(workItem) {
        log.dbg(`add work`)
        this.waitingQueue.push(workItem)
        this.startWork()
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

        let batch = this.waitingQueue
        this.waitingQueue = []

        log.dbg(`start work of ${batch.length} items`)
        this.workInProgress = this.worker(batch).then(() => {
            this.workInProgress = null

            log.dbg(`finished work of ${batch.length} items`)

            if (this.waitingQueue.length == 0)
                this.signalEndWaiters()
            else
                this.startWork()
        }).catch((error) => {
            this.workInProgress = null

            log.err(error)
            log.dbg(`finished with ERROR work of ${batch.length} items`)

            if (this.waitingQueue.length == 0)
                this.signalEndWaiters()
            else
                this.startWork()
        })
    }

    private signalEndWaiters() {
        log.dbg(`signal end`)
        let len = this.finishWaiters.length
        while (len-- > 0)
            this.finishWaiters.shift()()
    }
}