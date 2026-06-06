import { Command, CommandContext, CommandResult, Transport } from '../../types'
import {
  CommandRequest,
  RedisKernel,
} from '../../commanders/custom/redis-kernel'
import { SessionState } from './session-state'
import { CapturingTransport } from './capturing-transport'
import { UserFacedError } from '../errors'

/**
 * Session represents a single client connection's execution state.
 * It uses the State pattern to handle MULTI/EXEC transactions,
 * and submits commands to the kernel for serialized execution.
 */
export class Session {
  private currentState: SessionState
  private static connectionCounter = 0
  private readonly connectionId: string
  private readonly commands: Record<string, Command>
  private luaCommands?: Record<string, Command>

  constructor(
    commands: Record<string, Command>,
    private readonly kernel: RedisKernel,
    initialState: SessionState,
  ) {
    this.commands = commands
    this.currentState = initialState
    this.connectionId = `conn-${++Session.connectionCounter}`
  }

  setLuaCommands(luaCommands: Record<string, Command>): void {
    this.luaCommands = luaCommands
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
      const request = {
        ...transition.executeCommand,
        signal,
      }
      const turn = await this.kernel.waitTurn()
      try {
        this.executeCommand(
          request.transport,
          request.command,
          request.args,
          request.signal,
        )
      } finally {
        turn.release()
      }
      return
    }

    // Execute batch (EXEC) if needed
    if (transition.executeBatch) {
      const turn = await this.kernel.waitTurn()
      try {
        this.executeBatch(transport, signal, transition.executeBatch)
      } finally {
        turn.release()
      }
      return
    }
  }

  /**
   * Execute a batch of commands atomically.
   * This is called when processing an EXEC command.
   */
  private executeBatch(
    transport: Transport,
    signal: AbortSignal,
    batch: CommandRequest[],
  ): void {
    const results: CommandResult[] = []

    // Execute all commands in the batch without yielding to other jobs
    for (const req of batch) {
      // Use a capturing transport to collect the result
      const capturingTransport = new CapturingTransport()

      try {
        const ctx: CommandContext = {
          commands: this.commands,
          luaCommands: this.luaCommands,
          signal,
          transport: capturingTransport,
        }

        const cmdName = req.command.toString().toLowerCase()
        const cmd = this.commands[cmdName]
        if (cmd) {
          cmd.run(req.command, req.args, ctx)
        }

        // Get the captured results
        const capturedResult = capturingTransport.getResults()
        results.push(capturedResult)

        // If the command closed the connection, stop execution
        if (capturingTransport.isClosed()) {
          transport.write(results)
          transport.flush({ close: true })
          return
        }
      } catch (err) {
        if (err instanceof UserFacedError) {
          // Errors during execution are added to results
          results.push(err)
        }

        throw err
      }
    }

    // Write all results as an array
    transport.write(results)
    transport.flush()
  }

  getConnectionId(): string {
    return this.connectionId
  }

  private executeCommand(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): void {
    const capturingTransport = new CapturingTransport()

    try {
      const ctx: CommandContext = {
        commands: this.commands,
        luaCommands: this.luaCommands,
        signal,
        transport: capturingTransport,
      }

      const cmdName = rawCmd.toString().toLowerCase()
      const cmd = this.commands[cmdName]

      if (cmd) {
        cmd.run(rawCmd, args, ctx)
      }

      // Get the captured results
      const capturedResult = capturingTransport.getResults()
      transport.write(capturedResult)
      transport.flush({ close: capturingTransport.isClosed() })
    } catch (err) {
      if (err instanceof UserFacedError) {
        transport.write(err)
        transport.flush()
      }

      throw err
    }
  }

  async shutdown(): Promise<void> {
    // No specific shutdown logic needed for commands map
  }
}
