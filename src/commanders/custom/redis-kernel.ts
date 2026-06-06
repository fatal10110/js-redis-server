import { Transport } from '../../types'

export interface CommandRequest {
  command: Buffer
  args: Buffer[]
  transport: Transport
  signal: AbortSignal
}

export interface CommandJob {
  id: string
  connectionId: string
  request: CommandRequest
  resolve: () => void
  reject: (error: Error) => void
  batch?: CommandRequest[]
}

export type JobHandlerResult = void | { suspended: Promise<void> }

type CommandJobHandler = (
  job: CommandJob,
) => JobHandlerResult | Promise<JobHandlerResult>

export type ReleaseTurn = () => void

export type TurnHandle = {
  release: ReleaseTurn
  suspend: (waitFor: Promise<unknown>) => Promise<TurnHandle>
}

type TurnResolver = () => void

export class RedisKernel {
  private queue: TurnResolver[] = []
  private isLocked = false
  private jobQueue: CommandJob[] = []
  private isProcessingJobs = false
  private readonly suspendedJobs = new Map<string, CommandJob>()

  constructor(private readonly handler?: CommandJobHandler) {}

  submit(job: CommandJob): void {
    if (!this.handler) {
      throw new Error('RedisKernel.submit requires a job handler')
    }

    this.jobQueue.push(job)
    this.scheduleJobProcessing()
  }

  getSuspendedCount(): number {
    return this.suspendedJobs.size
  }

  /**
   * Wait for your turn in the global queue. The returned release
   * callback must be called when the command is finished executing.
   */
  waitTurn(): Promise<TurnHandle> {
    return this.waitTurnInternal(false)
  }

  private enqueue(grantTurn: TurnResolver, front: boolean) {
    if (front) {
      this.queue.unshift(grantTurn)
    } else {
      this.queue.push(grantTurn)
    }
  }

  private waitTurnInternal(priority: boolean): Promise<TurnHandle> {
    return new Promise(resolve => {
      const grantTurn = () => {
        this.isLocked = true
        let released = false
        const release: ReleaseTurn = () => {
          if (released) return
          released = true
          this.isLocked = false
          this.scheduleNext()
        }
        const suspend = (waitFor: Promise<unknown>): Promise<TurnHandle> => {
          if (released) {
            return Promise.reject(new Error('Turn already released'))
          }
          released = true
          this.isLocked = false
          this.scheduleNext()
          return waitFor.then(() => this.waitTurnInternal(true))
        }
        resolve({ release, suspend })
      }

      this.enqueue(grantTurn, priority)
      this.scheduleNext()
    })
  }

  private scheduleJobProcessing(): void {
    if (!this.isProcessingJobs && this.jobQueue.length > 0) {
      setImmediate(() => this.processJobs())
    }
  }

  private async processJobs(): Promise<void> {
    const handler = this.handler
    if (!handler || this.isProcessingJobs) return

    this.isProcessingJobs = true

    while (this.jobQueue.length > 0) {
      const job = this.jobQueue.shift()
      if (!job) continue

      try {
        const result = await handler(job)
        if (result && 'suspended' in result) {
          this.suspendedJobs.set(job.id, job)
          result.suspended
            .then(() => {
              this.suspendedJobs.delete(job.id)
              job.resolve()
              this.scheduleJobProcessing()
            })
            .catch(err => {
              this.suspendedJobs.delete(job.id)
              job.reject(err instanceof Error ? err : new Error(String(err)))
              this.scheduleJobProcessing()
            })
          continue
        }

        job.resolve()
      } catch (err) {
        job.reject(err instanceof Error ? err : new Error(String(err)))
      }
    }

    this.isProcessingJobs = false
    this.scheduleJobProcessing()
  }

  private scheduleNext() {
    if (this.isLocked) return
    const next = this.queue.shift()
    if (next) {
      next()
    }
  }
}
