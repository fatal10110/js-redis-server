import { Command, Transport } from '../../types'
import { CommandJob, RedisKernel } from '../../commanders/custom/redis-kernel'
import { SessionState } from './session-state'
import { CapturingTransport } from './capturing-transport'
import { CommandExecutionContext } from '../../commanders/custom/execution-context'

/**
 * Session represents a single client connection's execution state.
 * It uses the State pattern to handle MULTI/EXEC transactions,
 * and submits commands to the kernel for serialized execution.
 */
export class Session {
  private currentState: SessionState
  private static connectionCounter = 0
  private jobCounter = 0
  private readonly connectionId: string
  private readonly context: CommandExecutionContext

  constructor(
    commands: Record<string, Command>,
    private readonly kernel: RedisKernel,
    initialState: SessionState,
  ) {
    this.context = new CommandExecutionContext(commands)
    this.currentState = initialState
    this.connectionId = `conn-${++Session.connectionCounter}`
  }

  /**
   * Handle a single command from the client.
   * The state machine determines whether to execute immediately or buffer for transaction.
   */
  async handle(
    transport: Transport,
    command: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    // Let the state machine handle the command
    const transition = this.currentState.handle(transport, command, args)

    // Update state
    this.currentState = transition.nextState

    // Execute single command if needed
    if (transition.executeCommand) {
      const jobId = `job-${++this.jobCounter}`
      return new Promise((resolve, reject) => {
        const job: CommandJob = {
          id: jobId,
          connectionId: this.connectionId,
          request: {
            ...transition.executeCommand!,
            signal,
          },
          resolve,
          reject,
        }
        this.kernel.submit(job)
      })
    }

    // Execute batch (EXEC) if needed
    if (transition.executeBatch) {
      const jobId = `job-${++this.jobCounter}`
      return new Promise((resolve, reject) => {
        const job: CommandJob = {
          id: jobId,
          connectionId: this.connectionId,
          request: {
            command: Buffer.from('__EXEC_BATCH__'),
            args: [],
            transport,
            signal,
          },
          resolve,
          reject,
          batch: transition.executeBatch,
        }
        this.kernel.submit(job)
      })
    }
  }

  /**
   * Execute a job (called by the kernel's handler).
   * Handles both single commands and batched transactions.
   */
  async executeJob(job: CommandJob): Promise<void> {
    // Handle batch execution (EXEC)
    if (job.batch) {
      await this.executeBatch(job)
      return
    }

    // Handle single command execution
    const req = job.request
    await this.executeCommand(req.transport, req.command, req.args, req.signal)
  }

  /**
   * Execute a batch of commands atomically.
   * This is called when processing an EXEC command.
   */
  private async executeBatch(job: CommandJob): Promise<void> {
    const { transport } = job.request
    const batch = job.batch!
    const results: unknown[] = []

    // Execute all commands in the batch without yielding to other jobs
    for (const req of batch) {
      // Use a capturing transport to collect the result
      const capturingTransport = new CapturingTransport()

      try {
        await this.executeCommand(
          capturingTransport,
          req.command,
          req.args,
          req.signal,
        )

        // Get the captured results
        const capturedResults = capturingTransport.getResults()
        if (capturedResults.length > 0) {
          // Take the first result (commands typically write once)
          results.push(capturedResults[0])
        } else {
          results.push(null)
        }

        // If the command closed the connection, stop execution
        if (capturingTransport.isClosed()) {
          transport.write(results, true)
          return
        }
      } catch (err) {
        // Errors during execution are added to results
        results.push(err)
      }
    }

    // Write all results as an array
    transport.write(results)
  }

  getConnectionId(): string {
    return this.connectionId
  }

  private async executeCommand(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    await this.context.execute(transport, rawCmd, args, signal)
  }

  async shutdown(): Promise<void> {
    // No specific shutdown logic needed for commands map
  }
}
