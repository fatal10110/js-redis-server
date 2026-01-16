import { UnknownCommand, UserFacedError } from '../../core/errors'
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
      return this
    }

    try {
      const res = cmd.run(rawCmd, args, signal)
      transport.write(res.response, res.close)
    } catch (err) {
      if (err instanceof UserFacedError) {
        transport.write(err)
        return this
      }

      throw err
    }

    return this
  }
}
