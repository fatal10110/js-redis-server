import { LuaEngine } from 'wasmoon'
import { Command } from '../../../../types'
import { DB } from '../../db'
import type { DiscoveryService } from '../../../../types'

// Basic Redis commands
export { default as createEval } from './eval'
export { default as createClient } from './client'
export { default as createCommandInfo } from './command'
export { default as createInfo } from './info'
export { default as createPing } from './ping'
export { default as createQuit } from './quit'

// All data commands (strings, keys, hashes, lists, sets, zsets)
export * from './data'

// Import all command creators for the createCommands function
import createEval from './eval'
import createClient from './client'
import createCommandInfo from './command'
import createInfo from './info'
import createPing from './ping'
import createQuit from './quit'
import createCluster from './cluster'

import {
  // String commands
  createGet,
  createSet,
  createMget,
  createMset,
  createMsetnx,
  createGetset,
  createAppend,
  createStrlen,
  createIncr,
  createDecr,
  createIncrby,
  createDecrby,
  createIncrbyfloat,
  // Key commands
  createDel,
  createExists,
  createType,
  createTtl,
  createPttl,
  createExpire,
  createExpireat,
  createFlushdb,
  createFlushall,
  createDbsize,
  // Hash commands
  createHset,
  createHget,
  createHdel,
  createHgetall,
  createHmget,
  createHmset,
  createHkeys,
  createHvals,
  createHlen,
  createHexists,
  createHincrby,
  createHincrbyfloat,
  // List commands
  createLpush,
  createRpush,
  createLpop,
  createRpop,
  createLlen,
  createLrange,
  createLindex,
  createLset,
  createLrem,
  createLtrim,
  // Set commands
  createSadd,
  createSrem,
  createScard,
  createSmembers,
  createSismember,
  createSpop,
  createSrandmember,
  createSdiff,
  createSinter,
  createSunion,
  createSmove,
  // Sorted set commands
  createZadd,
  createZrem,
  createZrange,
  createZrevrange,
  createZrank,
  createZrevrank,
  createZscore,
  createZcard,
  createZincrby,
  createZrangebyscore,
  createZremrangebyscore,
} from './data'

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
    flushdb: createFlushdb(db),
    flushall: createFlushall(db),
    dbsize: createDbsize(db),
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

export function createClusterCommands(
  db: DB,
  luaEngine: LuaEngine,
  discoveryService: DiscoveryService,
  mySelfId: string,
): Record<string, Command> {
  return {
    ...createCommands(luaEngine, db),
    cluster: createCluster(discoveryService, mySelfId),
  }
}

/**
 * Create a filtered set of readonly commands safe for replicas
 * These commands don't modify data and are safe for read-only operations
 */
export function createReadonlyCommands(
  luaEngine: LuaEngine,
  db: DB,
): Record<string, Command> {
  const allCommands = createCommands(luaEngine, db)
  const readonlyCommandNames = new Set([
    // String readonly commands
    'get',
    'mget',
    'strlen',

    // Key readonly commands
    'exists',
    'type',
    'ttl',
    'pttl',
    'dbsize',

    // Hash readonly commands
    'hget',
    'hgetall',
    'hmget',
    'hkeys',
    'hvals',
    'hlen',
    'hexists',

    // List readonly commands
    'llen',
    'lrange',
    'lindex',

    // Set readonly commands
    'scard',
    'smembers',
    'sismember',
    'srandmember',
    'sdiff',
    'sinter',
    'sunion',

    // Sorted set readonly commands
    'zrange',
    'zrevrange',
    'zrank',
    'zrevrank',
    'zscore',
    'zcard',
    'zrangebyscore',

    // Server info commands
    'ping',
    'info',
    'command',
    'client',
  ])

  const readonlyCommands: Record<string, Command> = {}
  for (const [name, command] of Object.entries(allCommands)) {
    if (readonlyCommandNames.has(name)) {
      readonlyCommands[name] = command
    }
  }

  return readonlyCommands
}

/**
 * Create a filtered set of commands allowed within MULTI/EXEC transactions
 * Excludes connection-level commands and certain server commands
 */
export function createMultiCommands(
  luaEngine: LuaEngine,
  db: DB,
): Record<string, Command> {
  const allCommands = createCommands(luaEngine, db)
  const multiCommandNames = new Set([
    // String commands
    'get',
    'set',
    'mget',
    'mset',
    'msetnx',
    'getset',
    'append',
    'strlen',
    'incr',
    'decr',
    'incrby',
    'decrby',
    'incrbyfloat',

    // Key commands
    'del',
    'exists',
    'type',
    'ttl',
    'pttl',
    'expire',
    'expireat',
    'flushdb',
    'flushall',
    'dbsize',

    // Hash commands
    'hset',
    'hget',
    'hdel',
    'hgetall',
    'hmget',
    'hmset',
    'hkeys',
    'hvals',
    'hlen',
    'hexists',
    'hincrby',
    'hincrbyfloat',

    // List commands
    'lpush',
    'rpush',
    'lpop',
    'rpop',
    'llen',
    'lrange',
    'lindex',
    'lset',
    'lrem',
    'ltrim',

    // Set commands
    'sadd',
    'srem',
    'scard',
    'smembers',
    'sismember',
    'spop',
    'srandmember',
    'sdiff',
    'sinter',
    'sunion',
    'smove',

    // Sorted set commands
    'zadd',
    'zrem',
    'zrange',
    'zrevrange',
    'zrank',
    'zrevrank',
    'zscore',
    'zcard',
    'zincrby',
    'zrangebyscore',
    'zremrangebyscore',
  ])

  const multiCommands: Record<string, Command> = {}
  for (const [name, command] of Object.entries(allCommands)) {
    if (multiCommandNames.has(name)) {
      multiCommands[name] = command
    }
  }

  return multiCommands
}

/**
 * Create a filtered set of commands allowed within Lua scripts
 * Excludes non-deterministic commands and connection-level commands
 */
export function createLuaCommands(
  luaEngine: LuaEngine,
  db: DB,
): Record<string, Command> {
  const allCommands = createCommands(luaEngine, db)
  const luaCommandNames = new Set([
    // String commands (deterministic only)
    'get',
    'set',
    'mget',
    'mset',
    'getset',
    'append',
    'strlen',
    'incr',
    'decr',
    'incrby',
    'decrby',
    'incrbyfloat',

    // Key commands
    'del',
    'exists',
    'type',
    'ttl',
    'pttl',
    'expire',
    'expireat',
    'dbsize',

    // Hash commands
    'hset',
    'hget',
    'hdel',
    'hgetall',
    'hmget',
    'hmset',
    'hkeys',
    'hvals',
    'hlen',
    'hexists',
    'hincrby',
    'hincrbyfloat',

    // List commands
    'lpush',
    'rpush',
    'lpop',
    'rpop',
    'llen',
    'lrange',
    'lindex',
    'lset',
    'lrem',
    'ltrim',

    // Set commands (excluding non-deterministic ones like spop, srandmember)
    'sadd',
    'srem',
    'scard',
    'smembers',
    'sismember',
    'sdiff',
    'sinter',
    'sunion',
    'smove',

    // Sorted set commands
    'zadd',
    'zrem',
    'zrange',
    'zrevrange',
    'zrank',
    'zrevrank',
    'zscore',
    'zcard',
    'zincrby',
    'zrangebyscore',
    'zremrangebyscore',
  ])

  const luaCommands: Record<string, Command> = {}
  for (const [name, command] of Object.entries(allCommands)) {
    if (luaCommandNames.has(name)) {
      luaCommands[name] = command
    }
  }

  return luaCommands
}
