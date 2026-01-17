import {
  ClusterCommanderFactory,
  Command,
  DBCommandExecutor,
  DiscoveryService,
  Logger,
} from '../../types'
import { createClusterCommands, createMultiCommands } from './commands/redis'
import { DB } from './db'
import { NormalState } from '../../core/transports/session-state'
import { BaseCommander } from './base-commander'
import { RespAdapter } from '../../core/transports/resp2/adapter'
import { Socket } from 'net'
import { ClusterSlotOwnershipValidator } from './cluster-slot-ownership-validator'
import { ClusterSlotValidator } from './cluster-slot-validator'
import { CommandExecutionContext } from './execution-context'

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
  private readonly slotValidator: ClusterSlotValidator
  private readonly slotOwnershipValidator: ClusterSlotOwnershipValidator
  private readonly baseCommander: BaseCommander

  constructor(
    private readonly db: DB,
    private readonly discoveryService: DiscoveryService,
    private readonly mySelfId: string,
    private readonly commands: Record<string, Command>,
    private readonly transactionCommands: Record<string, Command>,
  ) {
    this.slotValidator = new ClusterSlotValidator(this.commands)
    this.slotOwnershipValidator = new ClusterSlotOwnershipValidator(
      this.discoveryService,
      this.mySelfId,
    )
    this.baseCommander = new BaseCommander(
      this.commands,
      validator =>
        new NormalState(
          validator,
          this.slotValidator,
          this.slotOwnershipValidator,
        ),
      commands =>
        new CommandExecutionContext(
          commands,
          this.slotValidator,
          this.slotOwnershipValidator,
        ),
    )
  }

  async shutdown(): Promise<void> {
    await this.baseCommander.shutdown()
  }

  /**
   * Creates a new RespAdapter for an incoming connection.
   * This is called by Resp2Transport when a new client connects.
   *
   * Uses NormalState with slot validation for cluster mode:
   * - Schema-driven slot validation via ClusterSlotValidator
   * - Slot pinning for transactions (all keys must hash to same slot)
   * - MOVED/CROSSSLOT error generation
   */
  createAdapter(logger: Logger, socket: Socket): RespAdapter {
    return this.baseCommander.createAdapter(logger, socket)
  }
}
