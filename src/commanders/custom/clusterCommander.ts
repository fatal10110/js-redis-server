import { Socket } from 'net'
import {
  ClusterCommanderFactory,
  Command,
  DBCommandExecutor,
  DiscoveryService,
  Logger,
} from '../../types'
import { createClusterCommands, createMultiCommands } from './commands/redis'
import { DB } from './db'
import { ClusterRouter } from './cluster-router'
import { CommandJob, RedisKernel } from './redis-kernel'
import { RespAdapter } from '../../core/transports/resp2/adapter'
import { Session } from '../../core/transports/session'
import { RegistryCommandValidator } from '../../core/transports/command-validator'
import { NormalState } from '../../core/transports/session-state'

export async function createCustomClusterCommander(
  logger: Logger,
  discoveryService: DiscoveryService,
): Promise<ClusterCommanderFactory> {
  return new CustomClusterCommanderFactory(logger, discoveryService)
}

export class CustomClusterCommanderFactory implements ClusterCommanderFactory {
  private readonly dbs: Record<string, DB> = {}

  constructor(
    private readonly logger: Logger,
    private readonly discoveryService: DiscoveryService,
  ) {}

  createCommander(mySelfId: string): DBCommandExecutor {
    this.dbs[mySelfId] = this.dbs[mySelfId] || new DB()
    const db = this.dbs[mySelfId]
    const commands = createClusterCommands(db, this.discoveryService, mySelfId)
    const transactionCommands = createMultiCommands(db)

    return new ClusterCommander(
      db,
      this.discoveryService,
      mySelfId,
      commands,
      transactionCommands,
    )
  }

  createReadOnlyCommander(mySelfId: string): DBCommandExecutor {
    const masterId = this.getMasterIdFromReplica(mySelfId)
    if (masterId) {
      return this.createCommander(masterId) // TODO readonly commander
    }

    const { id } = this.discoveryService.getMaster(mySelfId)
    return this.createCommander(id) // TODO readonly commander
  }

  shutdown(): Promise<void> {
    return Promise.resolve()
  }

  private getMasterIdFromReplica(replicaId: string): string | null {
    if (!replicaId.startsWith('replica-')) {
      return null
    }

    const parts = replicaId.split('-')
    if (parts.length < 3) {
      return null
    }

    return parts.slice(2).join('-')
  }
}

export class ClusterCommander implements DBCommandExecutor {
  private readonly kernel: RedisKernel
  private readonly sessions = new Map<string, Session>()
  private readonly baseValidator: RegistryCommandValidator
  private readonly router: ClusterRouter

  constructor(
    private readonly db: DB,
    private readonly discoveryService: DiscoveryService,
    private readonly mySelfId: string,
    private readonly commands: Record<string, Command>,
    private readonly transactionCommands: Record<string, Command>,
  ) {
    this.kernel = new RedisKernel(this.handleJob.bind(this))
    this.baseValidator = new RegistryCommandValidator(this.commands)
    this.router = new ClusterRouter(
      this.discoveryService,
      this.mySelfId,
      this.commands,
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
   *
   * Uses NormalState with slot validation for cluster mode:
   * - Schema-driven slot validation via ClusterRouter
   * - Slot pinning for transactions (all keys must hash to same slot)
   * - MOVED/CROSSSLOT error generation
   */
  createAdapter(logger: Logger, socket: Socket): RespAdapter {
    // Create initial state with slot validation for cluster mode
    const initialState = new NormalState(this.baseValidator, this.router)

    // Create session with cluster-aware state machine
    const session = new Session(this.commands, this.kernel, initialState)
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
