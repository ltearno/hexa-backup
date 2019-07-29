import { HexaBackupStore } from './HexaBackupStore'
import { Queue, LoggerBuilder, HashTools } from '@ltearno/hexa-js'
import * as Authorization from './Authorization'
import { spawn } from 'child_process'
import * as fs from 'fs'
import * as fsPath from 'path'
import * as Operations from './Operations'

const log = LoggerBuilder.buildLogger('bkgnd-jobs')

type JobBuilder = () => Promise<any>

interface Job<D, R> {
    clientName: string
    id: string
    name: string
    builder: JobBuilder
}

const uuid = () => HashTools.hashStringSync(`${Date.now()}-${Math.random()}-${Math.random()}`)

export interface BackgroundJobClientApi {
    // returns the job's internal id
    addJob<D, R>(name: string, builder: JobBuilder): string
}

export class BackgroundJobs {
    private jobQueue = new Queue.Queue<string>('bkgnd-jobs')
    private currentJob: Job<any, any> = null
    private waitingJobs: Job<any, any>[] = []

    constructor() {
        this.jobLoop()
    }

    addEnpointsToApp(app: any) {
        app.get('/jobs', async (req, res) => {
            res.set('Content-Type', 'application/json')
            res.send({
                running: this.currentJob,
                waiting: JSON.stringify(this.waitingJobs)
            })
        })
    }

    private async jobLoop() {
        const waiter = Queue.waitPopper(this.jobQueue)

        while (true) {
            log(`waiting for job`)
            try {
                const uuid = await waiter()
                if (!uuid) {
                    log(`finished job loop`)
                    break
                }

                log(`preparing for job ${uuid}`)

                const info = this.waitingJobs.shift()
                if (uuid != info.id) {
                    log.err(`DISCREPANCIES DKJHGKJHGDZZ ${uuid} / ${info.id}`)
                }

                log(`beginning job ${info.name} - ${info.id} (still ${this.waitingJobs.length} in queue)`)
                let result = undefined
                let error = undefined
                try {
                    result = await info.builder()
                }
                catch (err) {
                    error = err
                }
                log(`finished job ${info.name}, id:${info.id}, result:${result}, err:${error}`)
            }
            catch (err) {
                log(`error ${err}, quitting background jobs loop`)
                break
            }
        }
    }

    createClient(clientName: string): BackgroundJobClientApi {
        return {
            addJob: (name, builder) => this.addJob(clientName, name, builder)
        }
    }

    private addJob<D, R>(clientName: string, name: string, builder: JobBuilder): string {
        const id = uuid()

        const job = {
            clientName,
            id,
            name,
            builder
        }

        this.waitingJobs.push(job)
        this.jobQueue.push(id)

        return id
    }
}