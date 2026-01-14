import { ExecutionContext, Transport } from '../../types'
import { CommandJob, RedisKernel } from '../../commanders/custom/redis-kernel'

/**
 * Session represents a single client connection's execution state.
 * It tracks the ExecutionContext (which can transition between normal and transaction modes)
 * and submits commands to the kernel for serialized execution.
 */
export class Session {
  private currentContext: ExecutionContext
  private static connectionCounter = 0
  private jobCounter = 0
  private readonly connectionId: string

  constructor(
    private readonly baseContext: ExecutionContext,
    private readonly kernel: RedisKernel,
  ) {
    this.currentContext = baseContext
    this.connectionId = `conn-${++Session.connectionCounter}`
  }

  /**
   * Handle a single command from the client.
   * Submits the command to the kernel for serialized execution.
   */
  async handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    const jobId = `job-${++this.jobCounter}`

    return new Promise((resolve, reject) => {
      const job: CommandJob = {
        id: jobId,
        connectionId: this.connectionId,
        request: {
          command,
          args,
          transport,
          signal,
        },
        resolve,
        reject,
      }

      this.kernel.submit(job)
    })
  }

  /**
   * Execute a job (called by the kernel's handler).
   * Updates the execution context based on the command result.
   */
  async executeJob(job: CommandJob): Promise<void> {
    const { transport, command, args, signal } = job.request
    const nextContext = await this.currentContext.execute(
      transport,
      command,
      args,
      signal,
    )
    this.currentContext = nextContext
  }

  getConnectionId(): string {
    return this.connectionId
  }

  async shutdown(): Promise<void> {
    await this.currentContext.shutdown()
  }
}
