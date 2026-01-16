import { Socket } from 'net'
import { Command, DBCommandExecutor, Logger } from '../../types'

import { DB } from './db'

// Import createCommands function from Redis index
import { createCommands, createMultiCommands } from './commands/redis'
import { RespAdapter } from '../../core/transports/resp2/adapter'
import { NormalState } from '../../core/transports/session-state'
import { BaseCommander } from './base-commander'

export async function createCustomCommander(
  logger: Logger,
): Promise<CustomCommanderFactory> {
  return new CustomCommanderFactory(logger)
}

export class CustomCommanderFactory {
  private readonly db = new DB()

  constructor(private readonly logger: Logger) {}

  shutdown(): Promise<void> {
    this.logger.info('Shutting down CustomClusterCommanderFactory')
    return Promise.resolve()
  }

  createCommander(): DBCommandExecutor {
    return new Commander(this.db)
  }
}

class Commander implements DBCommandExecutor {
  private readonly commands: Record<string, Command>
  private readonly transactionCommands: Record<string, Command>
  private readonly baseCommander: BaseCommander

  constructor(db: DB) {
    this.commands = createCommands(db)
    this.transactionCommands = createMultiCommands(db)
    // Transaction state is now managed by Session, so no transactionCommands needed here
    this.baseCommander = new BaseCommander(
      this.commands,
      validator => new NormalState(validator),
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
