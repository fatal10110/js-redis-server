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
import { CommandJob, RedisKernel } from './redis-kernel'

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
  private readonly connectionContexts = new WeakMap<
    Transport,
    ExecutionContext
  >()
  private readonly connectionIds = new WeakMap<Transport, string>()
  private readonly kernel: RedisKernel
  private connectionCounter = 0
  private jobCounter = 0

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
    this.kernel = new RedisKernel(this.handleJob.bind(this))
  }

  async execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    const connectionId = this.getConnectionId(transport)
    const jobId = `job-${++this.jobCounter}`

    return new Promise((resolve, reject) => {
      const job: CommandJob = {
        id: jobId,
        connectionId,
        request: {
          command: rawCmd,
          args,
          transport,
          signal,
        },
        resolve,
        reject,
      }

      this.kernel.submit(job)
    })
  }

  async shutdown(): Promise<void> {
    return Promise.resolve()
  }

  private getConnectionId(transport: Transport): string {
    const existing = this.connectionIds.get(transport)
    if (existing) return existing

    const id = `conn-${++this.connectionCounter}`
    this.connectionIds.set(transport, id)
    return id
  }

  private async handleJob(job: CommandJob): Promise<void> {
    const { transport, command, args, signal } = job.request
    const context = this.connectionContexts.get(transport) ?? this.baseContext
    const nextContext = await context.execute(transport, command, args, signal)
    this.connectionContexts.set(transport, nextContext)
  }
}
