import { Socket } from 'net'
import { LuaEngine, LuaFactory } from 'wasmoon'
import { DBCommandExecutor, Logger } from '../../types'

import { DB } from './db'

// Import createCommands function from Redis index
import { createCommands, createMultiCommands } from './commands/redis'
import { CommandExecutionContext } from './execution-context'
import { CommandJob, RedisKernel } from './redis-kernel'
import { RespAdapter } from '../../core/transports/resp2/adapter'
import { Session } from '../../core/transports/session'

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
  private readonly kernel: RedisKernel
  private readonly sessions = new Map<string, Session>()

  constructor(luaEngine: LuaEngine, db: DB) {
    const commands = createCommands(luaEngine, db)
    const transactionCommands = createMultiCommands(luaEngine, db)
    this.baseContext = new CommandExecutionContext(
      db,
      commands,
      transactionCommands,
    )
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
   * Execute method is not used in Phase 4 architecture.
   * Use createAdapter() instead for real connections.
   */
  async execute(): Promise<void> {
    throw new Error(
      'Direct execute() is not supported in Phase 4 architecture. Use createAdapter() for connection-based execution.',
    )
  }

  /**
   * Creates a new RespAdapter for an incoming connection.
   * This is called by Resp2Transport when a new client connects.
   */
  createAdapter(logger: Logger, socket: Socket): RespAdapter {
    // Create session and register it
    const session = new Session(this.baseContext, this.kernel)
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
