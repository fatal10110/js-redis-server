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
}

type CommandJobHandler = (job: CommandJob) => Promise<void>

export class RedisKernel {
  private queue: CommandJob[] = []
  private isProcessing = false

  constructor(private readonly handler: CommandJobHandler) {}

  submit(job: CommandJob) {
    this.queue.push(job)

    if (!this.isProcessing) {
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
        await this.handler(job)
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
}
