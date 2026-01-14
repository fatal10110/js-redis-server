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
  /** Optional batch of commands to execute atomically (for EXEC) */
  batch?: CommandRequest[]
}

/**
 * Result returned by command handler indicating job status.
 * - void/undefined: Job completed normally
 * - { suspended: Promise }: Job is parked waiting for an event (e.g., BLPOP)
 */
export type JobHandlerResult = void | { suspended: Promise<void> }

type CommandJobHandler = (job: CommandJob) => Promise<JobHandlerResult>

export class RedisKernel {
  private queue: CommandJob[] = []
  private isProcessing = false
  /** Jobs that are suspended waiting for events (e.g., BLPOP waiting for list push) */
  private suspendedJobs = new Map<string, CommandJob>()

  constructor(private readonly handler: CommandJobHandler) {}

  submit(job: CommandJob) {
    this.queue.push(job)

    if (!this.isProcessing) {
      setImmediate(() => this.processLoop())
    }
  }

  /**
   * Resume processing loop after a suspended job completes.
   * Called when a suspended job's promise resolves.
   */
  private scheduleProcessing() {
    if (!this.isProcessing && this.queue.length > 0) {
      setImmediate(() => this.processLoop())
    }
  }

  private async processLoop() {
    if (this.isProcessing) return
    this.isProcessing = true

    while (this.queue.length > 0) {
      const job = this.queue.shift()

      if (!job) {
        continue
      }

      try {
        const result = await this.handler(job)

        if (result && 'suspended' in result) {
          // Job is suspended - park it and continue processing other jobs
          this.suspendedJobs.set(job.id, job)

          // Handle suspended job completion asynchronously
          result.suspended
            .then(() => {
              this.suspendedJobs.delete(job.id)
              job.resolve()
              this.scheduleProcessing()
            })
            .catch(err => {
              this.suspendedJobs.delete(job.id)
              const error = err instanceof Error ? err : new Error(String(err))
              job.reject(error)
              this.scheduleProcessing()
            })

          // Continue processing other jobs without waiting
          continue
        }

        // Job completed normally
        job.resolve()
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err))
        job.reject(error)
      }
    }

    this.isProcessing = false

    if (this.queue.length > 0) {
      // Defensive: if future changes add an await before isProcessing flips, avoid missed jobs.
      setImmediate(() => this.processLoop())
    }
  }

  /**
   * Get the number of currently suspended jobs
   */
  getSuspendedCount(): number {
    return this.suspendedJobs.size
  }
}
