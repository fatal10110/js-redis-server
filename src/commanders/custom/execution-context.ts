import { UnknownCommand, UserFacedError } from '../../core/errors'
import { BufferedTransport } from '../../core/transports/buffered-transport'
import {
  Command,
  CommandContext,
  DiscoveryService,
  ExecutionContext,
  Transport,
} from '../../types'
import type { DB } from './db'
import type { LuaRuntime } from './lua-runtime'

export interface ExecutionContextOptions {
  db: DB
  discoveryService?: DiscoveryService
  mySelfId?: string
  luaRuntime?: LuaRuntime
}

/**
 * CommandExecutionContext handles single command execution.
 * Transaction state (MULTI/EXEC) is now managed by Session's state machine.
 */
export class CommandExecutionContext implements ExecutionContext {
  private readonly commands: Record<string, Command>
  private luaCommands?: Record<string, Command>
  private readonly options: ExecutionContextOptions

  constructor(
    commands: Record<string, Command>,
    options: ExecutionContextOptions,
  ) {
    this.commands = commands
    this.options = options
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
        db: this.options.db,
        discoveryService: this.options.discoveryService,
        mySelfId: this.options.mySelfId,
        luaRuntime: this.options.luaRuntime,
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
