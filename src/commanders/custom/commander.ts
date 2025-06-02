import { LuaEngine, LuaFactory } from 'wasmoon'
import { UnknownCommand } from '../../core/errors'
import { Command, CommandResult, DBCommandExecutor, Logger } from '../../types'
import createEval from './commands/redis/eval'
import createClient from './commands/redis/client'
import createGet from './commands/redis/data/get'
import createSet from './commands/redis/data/set'
import createMget from './commands/redis/data/mget'
import createDel from './commands/redis/data/del'
import createCommandInfo from './commands/redis/command'
import createInfo from './commands/redis/info'
import createPing from './commands/redis/ping'
import createQuit from './commands/redis/quit'
import { DB } from './db'
import { TransactionCommand } from './transaction'

export function createCommands(
  luaEngine: LuaEngine,
  db: DB,
): Record<string, Command> {
  let commands: Record<string, Command> = {
    ping: createPing(),
    quit: createQuit(),
    client: createClient(),
    get: createGet(db),
    set: createSet(db),
    mget: createMget(db),
    del: createDel(db),
    command: createCommandInfo(),
    info: createInfo(),
  }

  commands = {
    ...commands,
    eval: createEval(luaEngine, commands),
  }

  return {
    ...commands,
  }
}

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
  private transactionCommand: TransactionCommand | null = null
  private readonly commands: Record<string, Command>

  constructor(luaEngine: LuaEngine, db: DB) {
    this.commands = createCommands(luaEngine, db)
  }

  async shutdown(): Promise<void> {
    return Promise.resolve()
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
      res = await this.transactionCommand!.run(rawCmd, args)
    } catch (err) {
      this.transactionCommand = null
      throw err
    }

    if (cmdName === 'exec') {
      this.transactionCommand = null
    }

    return res
  }

  execute(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    const cmdName = rawCmd.toString().toLowerCase()

    if (cmdName === 'multi') {
      this.transactionCommand = new TransactionCommand(this.commands)
      return Promise.resolve({ response: 'OK' })
    }

    if (this.transactionCommand) {
      return this.executeTransactionCommand(cmdName, rawCmd, args)
    }

    if (!this.commands[cmdName]) {
      throw new UnknownCommand(cmdName, args)
    }

    return Promise.resolve(this.commands[cmdName].run(rawCmd, args))
  }
}
