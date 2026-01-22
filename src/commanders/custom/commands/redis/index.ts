import { Command } from '../../../../types'
import type { DiscoveryService } from '../../../../types'
import { DB } from '../../db'
import type { LuaRuntime } from '../../lua-runtime'

// All data commands (strings, keys, hashes, lists, sets, zsets)
export * from './data'

export function createCommands(options: {
  db: DB
  luaRuntime?: LuaRuntime
  discoveryService?: DiscoveryService
  mySelfId?: string
}): Record<string, Command> {
  const registry = createCommandRegistry(options)
  return stripSubCommands(registry.toRecord())
}

export function createClusterCommands(
  db: DB,
  discoveryService: DiscoveryService,
  mySelfId: string,
  luaRuntime?: LuaRuntime,
): Record<string, Command> {
  return createCommands({ db, discoveryService, mySelfId, luaRuntime })
}

/**
 * Create a filtered set of readonly commands safe for replicas
 * These commands don't modify data and are safe for read-only operations
 */
export function createReadonlyCommands(db: DB): Record<string, Command> {
  const registry = createCommandRegistry({ db })
  const allCommands = stripSubCommands(registry.toRecord())
  const readonlyDefinitions = registry.getReadonlyCommands()
  const readonlyNames = new Set(
    readonlyDefinitions.map(def => def.metadata.name),
  )

  return filterCommands(allCommands, readonlyNames)
}

/**
 * Create a filtered set of commands allowed within MULTI/EXEC transactions
 * Excludes connection-level commands and certain server commands
 */
export function createMultiCommands(db: DB): Record<string, Command> {
  const registry = createCommandRegistry({ db })
  const allCommands = stripSubCommands(registry.toRecord())
  const multiDefinitions = registry.getMultiCommands()
  const multiNames = new Set(multiDefinitions.map(def => def.metadata.name))

  return filterCommands(allCommands, multiNames)
}

/**
 * Create a filtered set of commands allowed within Lua scripts
 * Excludes non-deterministic commands and connection-level commands
 */
export function createLuaCommands(options: {
  db: DB
}): Record<string, Command> {
  const registry = createCommandRegistry({ db: options.db })
  const allCommands = stripSubCommands(registry.toRecord())
  const luaDefinitions = registry.getLuaCommands()
  const luaNames = new Set(luaDefinitions.map(def => def.metadata.name))

  return filterCommands(allCommands, luaNames)
}

function filterCommands(
  commands: Record<string, Command>,
  allowedNames: Set<string>,
): Record<string, Command> {
  const filtered: Record<string, Command> = {}
  for (const [name, command] of Object.entries(commands)) {
    if (allowedNames.has(name)) {
      filtered[name] = command
    }
  }

  return filtered
}

function stripSubCommands(
  commands: Record<string, Command>,
): Record<string, Command> {
  const filtered: Record<string, Command> = {}

  for (const [name, command] of Object.entries(commands)) {
    if (!name.includes('|')) {
      filtered[name] = command
    }
  }

  return filtered
}

/**
 * Create and populate command registry
 */
import { CommandRegistry } from '../registry'

// String commands
import { GetCommand } from './data/strings/get'
import { SetCommand } from './data/strings/set'
import { MgetCommand } from './data/strings/mget'
import { MsetCommand } from './data/strings/mset'
import { MsetnxCommand } from './data/strings/msetnx'
import { GetsetCommand } from './data/strings/getset'
import { AppendCommand } from './data/strings/append'
import { StrlenCommand } from './data/strings/strlen'
import { IncrCommand } from './data/strings/incr'
import { DecrCommand } from './data/strings/decr'
import { IncrbyCommand } from './data/strings/incrby'
import { DecrbyCommand } from './data/strings/decrby'
import { IncrbyfloatCommand } from './data/strings/incrbyfloat'
import { SetnxCommand } from './data/strings/setnx'
import { SetexCommand } from './data/strings/setex'
import { PsetexCommand } from './data/strings/psetex'
import { GetdelCommand } from './data/strings/getdel'
import { GetexCommand } from './data/strings/getex'
import { SetrangeCommand } from './data/strings/setrange'
import { GetrangeCommand } from './data/strings/getrange'

// Key commands
import { DelCommand } from './data/keys/del'
import { ExistsCommand } from './data/keys/exists'
import { TypeCommand } from './data/keys/type'
import { TtlCommand } from './data/keys/ttl'
import { PttlCommand } from './data/keys/pttl'
import { ExpireCommand } from './data/keys/expire'
import { ExpireatCommand } from './data/keys/expireat'
import { FlushdbCommand } from './data/keys/flushdb'
import { FlushallCommand } from './data/keys/flushall'
import { DbSizeCommand } from './data/keys/dbsize'
import { RenameCommand } from './data/keys/rename'
import { RenamenxCommand } from './data/keys/renamenx'
import { PersistCommand } from './data/keys/persist'

// Hash commands
import { HsetCommand } from './data/hashes/hset'
import { HsetnxCommand } from './data/hashes/hsetnx'
import { HgetCommand } from './data/hashes/hget'
import { HdelCommand } from './data/hashes/hdel'
import { HgetallCommand } from './data/hashes/hgetall'
import { HmgetCommand } from './data/hashes/hmget'
import { HmsetCommand } from './data/hashes/hmset'
import { HkeysCommand } from './data/hashes/hkeys'
import { HvalsCommand } from './data/hashes/hvals'
import { HlenCommand } from './data/hashes/hlen'
import { HexistsCommand } from './data/hashes/hexists'
import { HincrbyCommand } from './data/hashes/hincrby'
import { HincrbyfloatCommand } from './data/hashes/hincrbyfloat'
import { HstrlenCommand } from './data/hashes/hstrlen'

// List commands
import { LpushCommand } from './data/lists/lpush'
import { RpushCommand } from './data/lists/rpush'
import { LpopCommand } from './data/lists/lpop'
import { RpopCommand } from './data/lists/rpop'
import { LlenCommand } from './data/lists/llen'
import { LrangeCommand } from './data/lists/lrange'
import { LindexCommand } from './data/lists/lindex'
import { LsetCommand } from './data/lists/lset'
import { LremCommand } from './data/lists/lrem'
import { LtrimCommand } from './data/lists/ltrim'
import { LpushxCommand } from './data/lists/lpushx'
import { RpushxCommand } from './data/lists/rpushx'
import { RpoplpushCommand } from './data/lists/rpoplpush'

// Set commands
import { SaddCommand } from './data/sets/sadd'
import { SremCommand } from './data/sets/srem'
import { ScardCommand } from './data/sets/scard'
import { SmembersCommand } from './data/sets/smembers'
import { SismemberCommand } from './data/sets/sismember'
import { SpopCommand } from './data/sets/spop'
import { SrandmemberCommand } from './data/sets/srandmember'
import { SdiffCommand } from './data/sets/sdiff'
import { SinterCommand } from './data/sets/sinter'
import { SunionCommand } from './data/sets/sunion'
import { SmoveCommand } from './data/sets/smove'
import { SdiffstoreCommand } from './data/sets/sdiffstore'
import { SinterstoreCommand } from './data/sets/sinterstore'
import { SunionstoreCommand } from './data/sets/sunionstore'

// Sorted set commands
import { ZaddCommand } from './data/zsets/zadd'
import { ZremCommand } from './data/zsets/zrem'
import { ZrangeCommand } from './data/zsets/zrange'
import { ZrevrangeCommand } from './data/zsets/zrevrange'
import { ZrankCommand } from './data/zsets/zrank'
import { ZrevrankCommand } from './data/zsets/zrevrank'
import { ZscoreCommand } from './data/zsets/zscore'
import { ZcardCommand } from './data/zsets/zcard'
import { ZincrbyCommand } from './data/zsets/zincrby'
import { ZrangebyscoreCommand } from './data/zsets/zrangebyscore'
import { ZremrangebyscoreCommand } from './data/zsets/zremrangebyscore'
import { ZcountCommand } from './data/zsets/zcount'
import { ZpopminCommand } from './data/zsets/zpopmin'
import { ZpopmaxCommand } from './data/zsets/zpopmax'

// Server/connection commands
import { PingCommand } from './ping'
import { InfoCommand } from './info'
import { QuitCommand } from './quit'
import { MonitorCommand } from './monitor'
import { CommandInfoCommand } from './command'
import { ClientCommand } from './client'
import { ClientSetNameCommand } from './client/clientSetName'

// Script commands
import { ScriptCommand } from './script'
import { ScriptLoadCommand } from './script/load'
import { ScriptExistsCommand } from './script/exists'
import { ScriptFlushCommand } from './script/flush'
import { ScriptKillCommand } from './script/kill'
import { ScriptDebugCommand } from './script/debug'
import { ScriptHelpCommand } from './script/help'
import { EvalCommand } from './script/eval'
import { EvalShaCommand } from './script/evalsha'

// Cluster commands
import { ClusterCommand } from './cluster'
import { ClusterInfoCommand } from './cluster/clusterInfo'
import { ClusterNodesCommand } from './cluster/clusterNodes'
import { ClusterSlotsCommand } from './cluster/clusterSlots'
import { ClusterShardsCommand } from './cluster/clusterShards'

export function createCommandRegistry(options: {
  db: DB
  discoveryService?: DiscoveryService
  mySelfId?: string
  luaRuntime?: LuaRuntime
}): CommandRegistry {
  const registry = new CommandRegistry()

  // String commands (20 commands)
  registry.registerAll([
    new GetCommand(options.db),
    new SetCommand(options.db),
    new MgetCommand(options.db),
    new MsetCommand(options.db),
    new MsetnxCommand(options.db),
    new GetsetCommand(options.db),
    new AppendCommand(options.db),
    new StrlenCommand(options.db),
    new IncrCommand(options.db),
    new DecrCommand(options.db),
    new IncrbyCommand(options.db),
    new DecrbyCommand(options.db),
    new IncrbyfloatCommand(options.db),
    new SetnxCommand(options.db),
    new SetexCommand(options.db),
    new PsetexCommand(options.db),
    new GetdelCommand(options.db),
    new GetexCommand(options.db),
    new SetrangeCommand(options.db),
    new GetrangeCommand(options.db),
  ])

  // Register commands that have been migrated to the new metadata system
  registry.registerAll([
    // Key commands (13 commands)
    new DelCommand(options.db),
    new ExistsCommand(options.db),
    new TypeCommand(options.db),
    new TtlCommand(options.db),
    new PttlCommand(options.db),
    new ExpireCommand(options.db),
    new ExpireatCommand(options.db),
    new FlushdbCommand(options.db),
    new FlushallCommand(options.db),
    new DbSizeCommand(options.db),
    new RenameCommand(options.db),
    new RenamenxCommand(options.db),
    new PersistCommand(options.db),
  ])

  registry.registerAll([
    // List commands (13 commands)
    new LpushCommand(options.db),
    new RpushCommand(options.db),
    new LpopCommand(options.db),
    new RpopCommand(options.db),
    new LlenCommand(options.db),
    new LrangeCommand(options.db),
    new LindexCommand(options.db),
    new LsetCommand(options.db),
    new LremCommand(options.db),
    new LtrimCommand(options.db),
    new LpushxCommand(options.db),
    new RpushxCommand(options.db),
    new RpoplpushCommand(options.db),

    // Set commands (14 commands)
    new SaddCommand(options.db),
    new SremCommand(options.db),
    new ScardCommand(options.db),
    new SmembersCommand(options.db),
    new SismemberCommand(options.db),
    new SpopCommand(options.db),
    new SrandmemberCommand(options.db),
    new SdiffCommand(options.db),
    new SinterCommand(options.db),
    new SunionCommand(options.db),
    new SmoveCommand(options.db),
    new SdiffstoreCommand(options.db),
    new SinterstoreCommand(options.db),
    new SunionstoreCommand(options.db),

    // Sorted set commands (14 commands)
    new ZaddCommand(options.db),
    new ZremCommand(options.db),
    new ZrangeCommand(options.db),
    new ZrevrangeCommand(options.db),
    new ZrankCommand(options.db),
    new ZrevrankCommand(options.db),
    new ZscoreCommand(options.db),
    new ZcardCommand(options.db),
    new ZincrbyCommand(options.db),
    new ZrangebyscoreCommand(options.db),
    new ZremrangebyscoreCommand(options.db),
    new ZcountCommand(options.db),
    new ZpopminCommand(options.db),
    new ZpopmaxCommand(options.db),

    // Hash commands (14 commands)
    new HsetCommand(options.db),
    new HsetnxCommand(options.db),
    new HgetCommand(options.db),
    new HdelCommand(options.db),
    new HgetallCommand(options.db),
    new HmgetCommand(options.db),
    new HmsetCommand(options.db),
    new HkeysCommand(options.db),
    new HvalsCommand(options.db),
    new HlenCommand(options.db),
    new HexistsCommand(options.db),
    new HincrbyCommand(options.db),
    new HincrbyfloatCommand(options.db),
    new HstrlenCommand(options.db),

    // Server/connection commands
    new PingCommand(),
    new InfoCommand(),
    new QuitCommand(),
    new MonitorCommand(),
    new CommandInfoCommand(),
    new ClientCommand(),
    new ClientSetNameCommand(),

    // Script commands
    new ScriptCommand(options.db),
    new ScriptLoadCommand(options.db),
    new ScriptExistsCommand(options.db),
    new ScriptFlushCommand(options.db),
    new ScriptKillCommand(),
    new ScriptDebugCommand(),
    new ScriptHelpCommand(),
    new EvalCommand(options.db, options.luaRuntime),
    new EvalShaCommand(options.db, options.luaRuntime),
  ])

  if (options.discoveryService && options.mySelfId) {
    registry.registerAll([
      new ClusterCommand(options.discoveryService, options.mySelfId),
      new ClusterInfoCommand(options.discoveryService, options.mySelfId),
      new ClusterNodesCommand(options.discoveryService, options.mySelfId),
      new ClusterSlotsCommand(options.discoveryService),
      new ClusterShardsCommand(options.discoveryService, options.mySelfId),
    ])
  }

  return registry
}
