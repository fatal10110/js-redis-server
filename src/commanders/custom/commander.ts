import { Socket } from 'net'
import { LuaEngine, LuaFactory } from 'wasmoon'
import { Command, DBCommandExecutor, Logger } from '../../types'
import { UnknownCommand, UserFacedError } from '../../core/errors'
import {
  Command,
  DBCommandExecutor,
  ExecutionContext,
  Logger,
  Transport,
} from '../../types'

import { DB } from './db'

// Import createCommands function from Redis index
import { createCommands, createMultiCommands } from './commands/redis'
import { CommandJob, RedisKernel } from './redis-kernel'
import { RespAdapter } from '../../core/transports/resp2/adapter'
import { Session } from '../../core/transports/session'
import { RegistryCommandValidator } from '../../core/transports/command-validator'
import { NormalState } from '../../core/transports/session-state'

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
  private readonly kernel: RedisKernel
  private readonly sessions = new Map<string, Session>()
  private readonly commands: Record<string, Command>
  private readonly transactionCommands: Record<string, Command>

  constructor(luaEngine: LuaEngine, db: DB) {
    this.commands = createCommands(luaEngine, db)
    this.transactionCommands = createMultiCommands(luaEngine, db)
    // Transaction state is now managed by Session, so no transactionCommands needed here
    this.kernel = new RedisKernel(this.handleJob.bind(this))
  }

  async shutdown(): Promise<void> {
    // Shutdown all sessions
    const shutdownPromises = Array.from(this.sessions.values()).map(session =>
      session.shutdown(),
    )
    await Promise.all(shutdownPromises)
    this.sessions.clear()
  }

  /**
   * Creates a new RespAdapter for an incoming connection.
   * This is called by Resp2Transport when a new client connects.
   */
  createAdapter(logger: Logger, socket: Socket): RespAdapter {
    // Create validator for ALL commands (not just transaction-safe ones)
    // The validator only checks syntax/arity, not whether commands are allowed
    const validator = new RegistryCommandValidator(this.commands)

    // Create session and register it
    const session = new Session(
      this.commands,
      this.kernel,
      new NormalState(validator),
    )
    const connectionId = session.getConnectionId()
    this.sessions.set(connectionId, session)

    // Clean up session when socket closes
    socket.on('close', () => {
      this.sessions.delete(connectionId)
      session.shutdown().catch(err => logger.error(err))
    })

    const adapter = new RespAdapter(logger, socket, session)
    return adapter
  }

  /**
   * Handle a job from the kernel by routing it to the appropriate session.
   */
  private async handleJob(job: CommandJob): Promise<void> {
    const session = this.sessions.get(job.connectionId)

    if (!session) {
      // Session not found - this shouldn't happen in normal operation
      // The session should be registered when the adapter is created
      throw new Error(`Session not found for connection ${job.connectionId}`)
    }

    await session.executeJob(job)
  }
}
