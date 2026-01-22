import { UnknownCommand, UserFacedError } from '../../core/errors'
import { BufferedTransport } from '../../core/transports/buffered-transport'
import {
  Command,
  CommandContext,
  ExecutionContext,
  Transport,
} from '../../types'

/**
 * CommandExecutionContext handles single command execution.
 * Transaction state (MULTI/EXEC) is now managed by Session's state machine.
 */
export class CommandExecutionContext implements ExecutionContext {
  private readonly commands: Record<string, Command>
  private luaCommands?: Record<string, Command>

  constructor(commands: Record<string, Command>) {
    this.commands = commands
  }

  setLuaCommands(luaCommands: Record<string, Command>): void {
    this.luaCommands = luaCommands
  }

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
      const ctx: CommandContext = {
        commands: this.commands,
        luaCommands: this.luaCommands,
        signal,
        transport: buffered,
      }
      cmd.run(rawCmd, args, ctx)
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
