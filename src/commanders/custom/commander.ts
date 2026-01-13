import { LuaEngine, LuaFactory } from 'wasmoon'
import {
  DBCommandExecutor,
  ExecutionContext,
  Logger,
  Transport,
} from '../../types'

import { DB } from './db'

// Import createCommands function from Redis index
import { createCommands, createMultiCommands } from './commands/redis'
import { CommandExecutionContext } from './execution-context'

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
  private readonly baseContext: CommandExecutionContext
  private currentContext: ExecutionContext

  constructor(luaEngine: LuaEngine, db: DB) {
    const commands = createCommands(luaEngine, db)
    const transactionCommands = createMultiCommands(luaEngine, db)
    this.baseContext = new CommandExecutionContext(
      db,
      commands,
      transactionCommands,
    )
    this.currentContext = this.baseContext
  }

  async shutdown(): Promise<void> {
    return Promise.resolve()
  }

  async execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    this.currentContext = await this.currentContext.execute(
      transport,
      rawCmd,
      args,
      signal,
    )
  }
}
