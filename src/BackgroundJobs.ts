import { HexaBackupStore } from './HexaBackupStore'
import { Queue, LoggerBuilder, HashTools } from '@ltearno/hexa-js'
import * as Authorization from './Authorization'
import { spawn } from 'child_process'
import * as fs from 'fs'
import * as fsPath from 'path'
import * as Operations from './Operations'

const log = LoggerBuilder.buildLogger('bkgnd-jobs')

type JobBuilder = () => Promise<any>

type JobWaiter = (result, error) => any | Promise<any>

interface Job {
    clientName: string
    id: string
    extId: string
    name: string
    builder: JobBuilder
    waiter: JobWaiter
}

const uuid = () => HashTools.hashStringSync(`${Date.now()}-${Math.random()}-${Math.random()}`)

export interface BackgroundJobClientApi {
    // returns the job's internal id
    addJob(extId: string, name: string, builder: JobBuilder, waiter: JobWaiter): string
}

export class BackgroundJobs {
    private jobQueue = new Queue.Queue<string>('bkgnd-jobs')
    private waitingJobs: Job[] = []

    constructor() {
        this.jobLoop()
    }

    private async jobLoop() {
        const waiter = Queue.waitPopper(this.jobQueue)

        while (true) {
            const uuid = await waiter()
            if (!uuid) {
                log(`finished job loop`)
                break
            }

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
                log.err(`in job "${info.name}": ${err}`)
                error = err
            }

            try {
                if (info.waiter) {
                    await info.waiter(result, error)
                }
            }
            catch (err) {
                log.err(`in job's waiter "${info.name}": ${err}`)
            }
            log(`finished job ${info.name} - ${info.id}`)
        }
    }

    createClient(clientName: string): BackgroundJobClientApi {
        return {
            addJob: (extId, name, builder, waiter) => this.addJob(clientName, extId, name, builder, waiter)
        }
    }

    private addJob(clientName: string, extId: string, name: string, builder: JobBuilder, waiter: JobWaiter): string {
        const id = uuid()

        const job = {
            clientName,
            id,
            extId,
            name,
            builder,
            waiter
        }

        this.waitingJobs.push(job)
        this.jobQueue.push(id)

        return id
    }
}