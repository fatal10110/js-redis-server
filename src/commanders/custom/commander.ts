import { LuaEngine, LuaFactory } from 'wasmoon'
import {
  DBCommandExecutor,
  ExecutionContext,
  Logger,
  Transport,
} from '../../types'

import { DB } from './db'

// Import createCommands function from Redis index
import { createCommands, createMultiCommands } from './commands/redis'
import { CommandExecutionContext } from './execution-context'
import { CommandJob, RedisKernel } from './redis-kernel'

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
  private readonly connectionContexts = new WeakMap<
    Transport,
    ExecutionContext
  >()
  private readonly connectionIds = new WeakMap<Transport, string>()
  private readonly kernel: RedisKernel
  private connectionCounter = 0
  private jobCounter = 0

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
    return Promise.resolve()
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
