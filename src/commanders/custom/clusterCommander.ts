import { Socket } from 'net'
import {
  ClusterCommanderFactory,
  Command,
  DBCommandExecutor,
  DiscoveryService,
  Logger,
} from '../../types'
import { createClusterCommands, createMultiCommands } from './commands/redis'
import { LuaEngine, LuaFactory } from 'wasmoon'
import { DB } from './db'
import { CommandExecutionContext } from './execution-context'
import { SlotValidator } from './slot-validation'
import { CommandJob, RedisKernel } from './redis-kernel'
import { RespAdapter } from '../../core/transports/resp2/adapter'
import { Session } from '../../core/transports/session'

export async function createCustomClusterCommander(
  logger: Logger,
  discoveryService: DiscoveryService,
): Promise<ClusterCommanderFactory> {
  const factory = new LuaFactory()
  const lua = await factory.createEngine({ injectObjects: true })
  return new CustomClusterCommanderFactory(logger, lua, discoveryService)
}

export class CustomClusterCommanderFactory implements ClusterCommanderFactory {
  private readonly dbs: Record<string, DB> = {}

  constructor(
    private readonly logger: Logger,
    private readonly luaEngine: LuaEngine,
    private readonly discoveryService: DiscoveryService,
  ) {}

  createCommander(mySelfId: string): DBCommandExecutor {
    this.dbs[mySelfId] = this.dbs[mySelfId] || new DB()
    const db = this.dbs[mySelfId]
    const commands = createClusterCommands(
      db,
      this.luaEngine,
      this.discoveryService,
      mySelfId,
    )
    const transactionCommands = createMultiCommands(this.luaEngine, db)

    return new ClusterCommander(
      db,
      this.discoveryService,
      mySelfId,
      commands,
      transactionCommands,
    )
  }

  createReadOnlyCommander(mySelfId: string): DBCommandExecutor {
    const { id } = this.discoveryService.getMaster(mySelfId)
    return this.createCommander(id) // TODO readonly commander
  }

  shutdown(): Promise<void> {
    this.luaEngine.global.close()
    return Promise.resolve()
  }
}

export class ClusterCommander implements DBCommandExecutor {
  private baseContext: CommandExecutionContext | null = null
  private readonly kernel: RedisKernel
  private readonly sessions = new Map<string, Session>()

  constructor(
    private readonly db: DB,
    private readonly discoveryService: DiscoveryService,
    private readonly mySelfId: string,
    private readonly commands: Record<string, Command>,
    private readonly transactionCommands: Record<string, Command>,
  ) {
    this.kernel = new RedisKernel(this.handleJob.bind(this))
  }

  /**
   * Lazy initialization of base context to avoid circular dependency.
   * The validator needs discovery service which needs transports to be initialized.
   */
  private getBaseContext(): CommandExecutionContext {
    if (!this.baseContext) {
      const me = this.discoveryService.getById(this.mySelfId)
      const validator = new SlotValidator(this.discoveryService, me)
      this.baseContext = new CommandExecutionContext(
        this.db,
        this.commands,
        this.transactionCommands,
        validator,
      )
    }
    return this.baseContext
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
    // Create session and register it
    const session = new Session(this.getBaseContext(), this.kernel)
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
