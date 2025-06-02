import { LuaEngine, LuaFactory } from 'wasmoon'
import { UnknownCommand } from '../../core/errors'
import { Command, CommandResult, DBCommandExecutor, Logger } from '../../types'
import createEval from './commands/redis/eval'
import createClient from './commands/redis/client'
import createGet from './commands/redis/data/strings/get'
import createSet from './commands/redis/data/strings/set'
import createMget from './commands/redis/data/strings/mget'
import createDel from './commands/redis/data/keys/del'
import createCommandInfo from './commands/redis/command'
import createInfo from './commands/redis/info'
import createPing from './commands/redis/ping'
import createQuit from './commands/redis/quit'
// String commands
import createIncr from './commands/redis/data/strings/incr'
import createDecr from './commands/redis/data/strings/decr'
import createAppend from './commands/redis/data/strings/append'
import createStrlen from './commands/redis/data/strings/strlen'
import createMset from './commands/redis/data/strings/mset'
import createMsetnx from './commands/redis/data/strings/msetnx'
import createGetset from './commands/redis/data/strings/getset'
import createIncrby from './commands/redis/data/strings/incrby'
import createDecrby from './commands/redis/data/strings/decrby'
import createIncrbyfloat from './commands/redis/data/strings/incrbyfloat'
// Key commands
import createExists from './commands/redis/data/keys/exists'
import createType from './commands/redis/data/keys/type'
import createTtl from './commands/redis/data/keys/ttl'
import createPttl from './commands/redis/data/keys/pttl'
import createExpire from './commands/redis/data/keys/expire'
import createExpireat from './commands/redis/data/keys/expireat'
// Hash commands
import createHset from './commands/redis/data/hashes/hset'
import createHget from './commands/redis/data/hashes/hget'
import createHdel from './commands/redis/data/hashes/hdel'
import createHgetall from './commands/redis/data/hashes/hgetall'
import createHmget from './commands/redis/data/hashes/hmget'
import createHmset from './commands/redis/data/hashes/hmset'
import createHkeys from './commands/redis/data/hashes/hkeys'
import createHvals from './commands/redis/data/hashes/hvals'
import createHlen from './commands/redis/data/hashes/hlen'
import createHexists from './commands/redis/data/hashes/hexists'
import createHincrby from './commands/redis/data/hashes/hincrby'
import createHincrbyfloat from './commands/redis/data/hashes/hincrbyfloat'
// List commands
import createLpush from './commands/redis/data/lists/lpush'
import createRpush from './commands/redis/data/lists/rpush'
import createLpop from './commands/redis/data/lists/lpop'
import createRpop from './commands/redis/data/lists/rpop'
import createLlen from './commands/redis/data/lists/llen'
import createLrange from './commands/redis/data/lists/lrange'
import createLindex from './commands/redis/data/lists/lindex'
import createLset from './commands/redis/data/lists/lset'
import createLrem from './commands/redis/data/lists/lrem'
import createLtrim from './commands/redis/data/lists/ltrim'
// Set commands
import createSadd from './commands/redis/data/sets/sadd'
import createSrem from './commands/redis/data/sets/srem'
import createScard from './commands/redis/data/sets/scard'
import createSmembers from './commands/redis/data/sets/smembers'
import createSismember from './commands/redis/data/sets/sismember'
import createSpop from './commands/redis/data/sets/spop'
import createSrandmember from './commands/redis/data/sets/srandmember'
import createSdiff from './commands/redis/data/sets/sdiff'
import createSinter from './commands/redis/data/sets/sinter'
import createSunion from './commands/redis/data/sets/sunion'
import createSmove from './commands/redis/data/sets/smove'
// Sorted set commands
import createZadd from './commands/redis/data/zsets/zadd'
import createZrem from './commands/redis/data/zsets/zrem'
import createZrange from './commands/redis/data/zsets/zrange'
import createZscore from './commands/redis/data/zsets/zscore'
import createZcard from './commands/redis/data/zsets/zcard'
import createZincrby from './commands/redis/data/zsets/zincrby'
import createZrevrange from './commands/redis/data/zsets/zrevrange'
import createZrank from './commands/redis/data/zsets/zrank'
import createZrevrank from './commands/redis/data/zsets/zrevrank'
import createZrangebyscore from './commands/redis/data/zsets/zrangebyscore'
import createZremrangebyscore from './commands/redis/data/zsets/zremrangebyscore'
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
    mset: createMset(db),
    msetnx: createMsetnx(db),
    getset: createGetset(db),
    incrby: createIncrby(db),
    decrby: createDecrby(db),
    incrbyfloat: createIncrbyfloat(db),
    // Key commands
    exists: createExists(db),
    type: createType(db),
    ttl: createTtl(db),
    pttl: createPttl(db),
    expire: createExpire(db),
    expireat: createExpireat(db),
    // Hash commands
    hset: createHset(db),
    hget: createHget(db),
    hdel: createHdel(db),
    hgetall: createHgetall(db),
    hmget: createHmget(db),
    hmset: createHmset(db),
    hkeys: createHkeys(db),
    hvals: createHvals(db),
    hlen: createHlen(db),
    hexists: createHexists(db),
    hincrby: createHincrby(db),
    hincrbyfloat: createHincrbyfloat(db),
    // List commands
    lpush: createLpush(db),
    rpush: createRpush(db),
    lpop: createLpop(db),
    rpop: createRpop(db),
    llen: createLlen(db),
    lrange: createLrange(db),
    lindex: createLindex(db),
    lset: createLset(db),
    lrem: createLrem(db),
    ltrim: createLtrim(db),
    // Set commands
    sadd: createSadd(db),
    srem: createSrem(db),
    scard: createScard(db),
    smembers: createSmembers(db),
    sismember: createSismember(db),
    spop: createSpop(db),
    srandmember: createSrandmember(db),
    sdiff: createSdiff(db),
    sinter: createSinter(db),
    sunion: createSunion(db),
    smove: createSmove(db),
    // Sorted set commands
    zadd: createZadd(db),
    zrem: createZrem(db),
    zrange: createZrange(db),
    zscore: createZscore(db),
    zcard: createZcard(db),
    zincrby: createZincrby(db),
    zrevrange: createZrevrange(db),
    zrank: createZrank(db),
    zrevrank: createZrevrank(db),
    zrangebyscore: createZrangebyscore(db),
    zremrangebyscore: createZremrangebyscore(db),
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
