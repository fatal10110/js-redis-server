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
// String commands
import createIncr from './commands/redis/data/incr'
import createDecr from './commands/redis/data/decr'
import createAppend from './commands/redis/data/append'
import createStrlen from './commands/redis/data/strlen'
// Key commands
import createExists from './commands/redis/data/exists'
import createType from './commands/redis/data/type'
// Hash commands
import createHset from './commands/redis/data/hset'
import createHget from './commands/redis/data/hget'
import createHdel from './commands/redis/data/hdel'
import createHgetall from './commands/redis/data/hgetall'
// List commands
import createLpush from './commands/redis/data/lpush'
import createRpush from './commands/redis/data/rpush'
import createLpop from './commands/redis/data/lpop'
import createRpop from './commands/redis/data/rpop'
import createLlen from './commands/redis/data/llen'
import createLrange from './commands/redis/data/lrange'
// Set commands
import createSadd from './commands/redis/data/sadd'
import createSrem from './commands/redis/data/srem'
import createScard from './commands/redis/data/scard'
import createSmembers from './commands/redis/data/smembers'
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
    // String commands
    incr: createIncr(db),
    decr: createDecr(db),
    append: createAppend(db),
    strlen: createStrlen(db),
    // Key commands
    exists: createExists(db),
    type: createType(db),
    // Hash commands
    hset: createHset(db),
    hget: createHget(db),
    hdel: createHdel(db),
    hgetall: createHgetall(db),
    // List commands
    lpush: createLpush(db),
    rpush: createRpush(db),
    lpop: createLpop(db),
    rpop: createRpop(db),
    llen: createLlen(db),
    lrange: createLrange(db),
    // Set commands
    sadd: createSadd(db),
    srem: createSrem(db),
    scard: createScard(db),
    smembers: createSmembers(db),
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
