import { UnknownCommand, UserFacedError } from '../../core/errors'
import { BufferedTransport } from '../../core/transports/buffered-transport'
import { Command, ExecutionContext, Transport } from '../../types'

/**
 * CommandExecutionContext handles single command execution.
 * Transaction state (MULTI/EXEC) is now managed by Session's state machine.
 */
export class CommandExecutionContext implements ExecutionContext {
  constructor(private readonly commands: Record<string, Command>) {}

  shutdown(): Promise<void> {
    return Promise.resolve()
  }

  execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): ExecutionContext {
    const cmdName = rawCmd.toString().toLowerCase()

    const cmd = this.commands[cmdName]

    if (!cmd) {
      transport.write(new UnknownCommand(cmdName, args))
      transport.flush()
      return this
    }

    try {
      const buffered = new BufferedTransport(transport)
      cmd.run(rawCmd, args, signal, buffered)
      buffered.flush()
    } catch (err) {
      if (err instanceof UserFacedError) {
        transport.write(err)
        transport.flush()
        return this
      }

      throw err
    }

    return this
  }
}
