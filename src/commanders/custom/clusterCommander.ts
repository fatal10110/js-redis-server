import { CorssSlot, MovedError, UnknownCommand } from '../../core/errors'
import clusterKeySlot from 'cluster-key-slot'
import {
  ClusterCommanderFactory,
  Command,
  CommandResult,
  DBCommandExecutor,
  DiscoveryNode,
  DiscoveryService,
  Logger,
} from '../../types'
import { createCommands } from './commander'
import { LuaEngine, LuaFactory } from 'wasmoon'
import { createCluster } from './commands/redis'
import { DB } from './db'
import { TransactionalCommander } from './commands/redis/multi'

export function createClusterCommands(
  db: DB,
  luaEngine: LuaEngine,
  transactionalCommander: TransactionalCommander,
  discoveryService: DiscoveryService,
  mySelfId: string,
): Record<string, Command> {
  return {
    ...createCommands(luaEngine, db, transactionalCommander),
    cluster: createCluster(discoveryService, mySelfId),
  }
}

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

    return new ClusterCommander(
      this.discoveryService.getById(mySelfId),
      this.discoveryService,
      createClusterCommands(
        this.dbs[mySelfId],
        this.luaEngine,
        this.discoveryService,
        mySelfId,
      ),
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

export class ClusterCommander
  implements DBCommandExecutor, TransactionalCommander
{
  private transaction: DBCommandExecutor | null = null

  constructor(
    private readonly me: DiscoveryNode,
    private readonly discoveryService: DiscoveryService,
    private readonly commands: Record<string, Command>,
  ) {}

  setTransaction(transaction: DBCommandExecutor | null): void {
    this.transaction = transaction
  }

  execute(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    const cmdName = rawCmd.toString().toLowerCase()
    const cmd = this.commands[cmdName]

    if (!cmd) {
      throw new UnknownCommand(cmdName, args)
    }

    const keys = cmd.getKeys(rawCmd, args)

    if (!keys.length) {
      return cmd.run(rawCmd, args)
    }

    const slot = clusterKeySlot.generateMulti(keys)

    if (slot === -1) {
      throw new CorssSlot()
    }

    for (const [min, max] of this.me.slots) {
      if (slot >= min && slot <= max) {
        return cmd.run(rawCmd, args)
      }
    }

    const clusterNode = this.discoveryService.getBySlot(slot)

    throw new MovedError(clusterNode.host, clusterNode.port, slot)
  }

  async shutdown(): Promise<void> {
    return Promise.resolve()
  }
}
