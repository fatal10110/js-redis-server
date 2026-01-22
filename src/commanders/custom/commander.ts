import { Socket } from 'net'
import { Command, DBCommandExecutor, Logger } from '../../types'

import { DB } from './db'

// Import createCommands function from Redis index
import { createCommands, createLuaCommands } from './commands/redis'
import { createLuaRuntime, LuaRuntime } from './lua-runtime'
import { RespAdapter } from '../../core/transports/resp2/adapter'
import { NormalState } from '../../core/transports/session-state'
import { BaseCommander } from './base-commander'

export async function createCustomCommander(
  logger: Logger,
): Promise<CustomCommanderFactory> {
  const luaRuntime = await createLuaRuntime(logger)
  return new CustomCommanderFactory(logger, luaRuntime)
}

export class CustomCommanderFactory {
  private readonly db = new DB()

  constructor(
    private readonly logger: Logger,
    private readonly luaRuntime: LuaRuntime,
  ) {}

  shutdown(): Promise<void> {
    this.logger.info('Shutting down CustomClusterCommanderFactory')
    return Promise.resolve()
  }

  createCommander(): DBCommandExecutor {
    return new Commander(this.db, this.luaRuntime)
  }
}

class Commander implements DBCommandExecutor {
  private readonly commands: Record<string, Command>
  private readonly luaCommands: Record<string, Command>
  private readonly baseCommander: BaseCommander
  private readonly db: DB

  constructor(db: DB, luaRuntime: LuaRuntime) {
    this.db = db
    this.commands = createCommands({ db, luaRuntime })
    this.luaCommands = createLuaCommands({ db })
    this.baseCommander = new BaseCommander(
      this.commands,
      validator => new NormalState(validator, this.db),
      this.luaCommands,
    )
  }

  async shutdown(): Promise<void> {
    await this.baseCommander.shutdown()
  }

  /**
   * Creates a new RespAdapter for an incoming connection.
   * This is called by Resp2Transport when a new client connects.
   */
  createAdapter(logger: Logger, socket: Socket): RespAdapter {
    return this.baseCommander.createAdapter(logger, socket)
  }
}
