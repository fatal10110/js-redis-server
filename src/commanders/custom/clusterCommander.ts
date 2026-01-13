import {
  ClusterCommanderFactory,
  Command,
  DBCommandExecutor,
  DiscoveryService,
  ExecutionContext,
  Logger,
  Transport,
} from '../../types'
import { createClusterCommands, createMultiCommands } from './commands/redis'
import { LuaEngine, LuaFactory } from 'wasmoon'
import { DB } from './db'
import { CommandExecutionContext } from './execution-context'
import { SlotValidator } from './slot-validation'

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
  private readonly baseContext: CommandExecutionContext
  private currentContext: ExecutionContext

  constructor(
    private readonly db: DB,
    private readonly discoveryService: DiscoveryService,
    private readonly mySelfId: string,
    private readonly commands: Record<string, Command>,
    private readonly transactionCommands: Record<string, Command>,
  ) {
    const me = this.discoveryService.getById(this.mySelfId)
    const validator = new SlotValidator(this.discoveryService, me)
    this.baseContext = new CommandExecutionContext(
      this.db,
      this.commands,
      this.transactionCommands,
      validator,
    )
    this.currentContext = this.baseContext
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

  async shutdown(): Promise<void> {
    return Promise.resolve()
  }
}
