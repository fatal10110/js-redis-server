import { LuaEngine, LuaFactory } from 'wasmoon'
import { UnknownCommand, UserFacedError } from '../../core/errors'
import { Command, DBCommandExecutor, Logger, Transport } from '../../types'

import { DB } from './db'
import { TransactionCommand } from './transaction'

// Import createCommands function from Redis index
import { createCommands } from './commands/redis'

export async function createCustomCommander(
  logger: Logger,
): Promise<CustomCommanderFactory> {
  const factory = new LuaFactory()
  const lua = await factory.createEngine({ injectObjects: true })

  return new CustomCommanderFactory(logger, lua)
}

export class CustomCommanderFactory {
  private readonly db = new DB()

  constructor(
    private readonly logger: Logger,
    private readonly luaEngine: LuaEngine,
  ) {}

  shutdown(): Promise<void> {
    this.logger.info('Shutting down CustomClusterCommanderFactory')
    this.luaEngine.global.close()
    return Promise.resolve()
  }

  createCommander(): DBCommandExecutor {
    return new Commander(this.luaEngine, this.db)
  }
}

class Commander implements DBCommandExecutor {
  private transactionCommand: TransactionCommand | null = null
  private readonly commands: Record<string, Command>

  constructor(luaEngine: LuaEngine, db: DB) {
    this.commands = createCommands(luaEngine, db)
  }

  async shutdown(): Promise<void> {
    return Promise.resolve()
  }

  private async executeTransactionCommand(
    transport: Transport,
    cmdName: string,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    if (cmdName === 'discard') {
      this.transactionCommand = null
      transport.write('OK')
      return
    }

    let res

    try {
      res = await this.transactionCommand!.run(rawCmd, args, signal)
    } catch (err) {
      this.transactionCommand = null

      if (err instanceof UserFacedError) {
        transport.write(err)
        return
      }

      throw err
    }

    if (cmdName === 'exec') {
      this.transactionCommand = null
    }

    transport.write(res.response, res.close)
    return
  }

  async execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    const cmdName = rawCmd.toString().toLowerCase()

    if (cmdName === 'multi') {
      this.transactionCommand = new TransactionCommand(this.commands)
      transport.write('OK')
      return Promise.resolve()
    }

    if (this.transactionCommand) {
      await this.executeTransactionCommand(
        transport,
        cmdName,
        rawCmd,
        args,
        signal,
      )
      return
    }

    if (!this.commands[cmdName]) {
      transport.write(new UnknownCommand(cmdName, args))
      return
    }

    const res = await this.commands[cmdName].run(rawCmd, args, signal)
    transport.write(res.response, res.close)
    return Promise.resolve()
  }
}
