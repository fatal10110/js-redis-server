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
import { createClusterCommands } from './commands/redis'
import { LuaEngine, LuaFactory } from 'wasmoon'
import { DB } from './db'
import { TransactionCommand } from './transaction'

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

export class ClusterCommander implements DBCommandExecutor {
  private transactionCommand: TransactionCommand | null = null

  constructor(
    private readonly me: DiscoveryNode,
    private readonly discoveryService: DiscoveryService,
    private readonly commands: Record<string, Command>,
  ) {}

  private executeCommand(
    cmd: Command,
    rawCmd: Buffer,
    args: Buffer[],
  ): Promise<CommandResult> {
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

  async executeTransactionCommand(
    cmdName: string,
    rawCmd: Buffer,
    args: Buffer[],
  ): Promise<CommandResult> {
    if (cmdName === 'discard') {
      this.transactionCommand = null
      return { response: 'OK' }
    }

    let res

    try {
      res = await this.executeCommand(this.transactionCommand!, rawCmd, args)
    } catch (err) {
      if (err instanceof MovedError) {
        this.transactionCommand = null
      }

      if (cmdName === 'exec') {
        this.transactionCommand = null
      }

      throw err
    }

    return res
  }

  async execute(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    const cmdName = rawCmd.toString().toLowerCase()

    if (this.transactionCommand) {
      return this.executeTransactionCommand(cmdName, rawCmd, args)
    }

    if (cmdName === 'multi') {
      this.transactionCommand = new TransactionCommand(this.commands)
      return { response: 'OK' }
    }

    const cmd = this.commands[cmdName]

    if (!cmd) {
      throw new UnknownCommand(cmdName, args)
    }

    return this.executeCommand(cmd, rawCmd, args)
  }

  async shutdown(): Promise<void> {
    return Promise.resolve()
  }
}
