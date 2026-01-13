import { LuaEngine } from 'wasmoon'
import { Command, ExecutionContext } from '../../../../types'
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

import createEval from './eval'
import createEvalSha from './evalsha'

export function createCommands(
  luaEngine: LuaEngine,
  db: DB,
  options?: {
    executionContext?: ExecutionContext
    discoveryService?: DiscoveryService
    mySelfId?: string
  },
): Record<string, Command> {
  const deps: CommandDependencies = {
    db,
    luaEngine,
    discoveryService: options?.discoveryService,
    mySelfId: options?.mySelfId,
  }
  const registry = createCommandRegistry(deps)
  const commands = stripSubCommands(registry.createCommands(deps))

  const evalCmd = createEval(luaEngine, commands, db, options?.executionContext)
  const evalsha = createEvalSha(evalCmd, db)

  return {
    ...commands,
    eval: evalCmd,
    evalsha,
  }
}

export function createClusterCommands(
  db: DB,
  luaEngine: LuaEngine,
  discoveryService: DiscoveryService,
  mySelfId: string,
): Record<string, Command> {
  return createCommands(luaEngine, db, { discoveryService, mySelfId })
}

/**
 * Create a filtered set of readonly commands safe for replicas
 * These commands don't modify data and are safe for read-only operations
 */
export function createReadonlyCommands(
  luaEngine: LuaEngine,
  db: DB,
): Record<string, Command> {
  const deps: CommandDependencies = { db, luaEngine }
  const registry = createCommandRegistry(deps)
  const allCommands = stripSubCommands(registry.createCommands(deps))
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
export function createMultiCommands(
  luaEngine: LuaEngine,
  db: DB,
): Record<string, Command> {
  const deps: CommandDependencies = { db, luaEngine }
  const registry = createCommandRegistry(deps)
  const allCommands = stripSubCommands(registry.createCommands(deps))
  const multiDefinitions = registry.getMultiCommands()
  const multiNames = new Set(multiDefinitions.map(def => def.metadata.name))

  return filterCommands(allCommands, multiNames)
}

/**
 * Create a filtered set of commands allowed within Lua scripts
 * Excludes non-deterministic commands and connection-level commands
 */
export function createLuaCommands(
  luaEngine: LuaEngine,
  db: DB,
): Record<string, Command> {
  const deps: CommandDependencies = { db, luaEngine }
  const registry = createCommandRegistry(deps)
  const allCommands = stripSubCommands(registry.createCommands(deps))
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
import type { CommandDependencies } from '../registry'

// String commands
import { GetCommandDefinition } from './data/strings/get'
import { SetCommandDefinition } from './data/strings/set'
import { MgetCommandDefinition } from './data/strings/mget'
import { MsetCommandDefinition } from './data/strings/mset'
import { MsetnxCommandDefinition } from './data/strings/msetnx'
import { GetsetCommandDefinition } from './data/strings/getset'
import { AppendCommandDefinition } from './data/strings/append'
import { StrlenCommandDefinition } from './data/strings/strlen'
import { IncrCommandDefinition } from './data/strings/incr'
import { DecrCommandDefinition } from './data/strings/decr'
import { IncrbyCommandDefinition } from './data/strings/incrby'
import { DecrbyCommandDefinition } from './data/strings/decrby'
import { IncrbyfloatCommandDefinition } from './data/strings/incrbyfloat'

// Key commands
import { DelCommandDefinition } from './data/keys/del'
import { ExistsCommandDefinition } from './data/keys/exists'
import { TypeCommandDefinition } from './data/keys/type'
import { TtlCommandDefinition } from './data/keys/ttl'
import { PttlCommandDefinition } from './data/keys/pttl'
import { ExpireCommandDefinition } from './data/keys/expire'
import { ExpireatCommandDefinition } from './data/keys/expireat'
import { FlushdbCommandDefinition } from './data/keys/flushdb'
import { FlushallCommandDefinition } from './data/keys/flushall'
import { DbSizeCommandDefinition } from './data/keys/dbsize'

// Hash commands
import { HsetCommandDefinition } from './data/hashes/hset'
import { HgetCommandDefinition } from './data/hashes/hget'
import { HdelCommandDefinition } from './data/hashes/hdel'
import { HgetallCommandDefinition } from './data/hashes/hgetall'
import { HmgetCommandDefinition } from './data/hashes/hmget'
import { HmsetCommandDefinition } from './data/hashes/hmset'
import { HkeysCommandDefinition } from './data/hashes/hkeys'
import { HvalsCommandDefinition } from './data/hashes/hvals'
import { HlenCommandDefinition } from './data/hashes/hlen'
import { HexistsCommandDefinition } from './data/hashes/hexists'
import { HincrbyCommandDefinition } from './data/hashes/hincrby'
import { HincrbyfloatCommandDefinition } from './data/hashes/hincrbyfloat'

// List commands
import { LpushCommandDefinition } from './data/lists/lpush'
import { RpushCommandDefinition } from './data/lists/rpush'
import { LpopCommandDefinition } from './data/lists/lpop'
import { RpopCommandDefinition } from './data/lists/rpop'
import { LlenCommandDefinition } from './data/lists/llen'
import { LrangeCommandDefinition } from './data/lists/lrange'
import { LindexCommandDefinition } from './data/lists/lindex'
import { LsetCommandDefinition } from './data/lists/lset'
import { LremCommandDefinition } from './data/lists/lrem'
import { LtrimCommandDefinition } from './data/lists/ltrim'

// Set commands
import { SaddCommandDefinition } from './data/sets/sadd'
import { SremCommandDefinition } from './data/sets/srem'
import { ScardCommandDefinition } from './data/sets/scard'
import { SmembersCommandDefinition } from './data/sets/smembers'
import { SismemberCommandDefinition } from './data/sets/sismember'
import { SpopCommandDefinition } from './data/sets/spop'
import { SrandmemberCommandDefinition } from './data/sets/srandmember'
import { SdiffCommandDefinition } from './data/sets/sdiff'
import { SinterCommandDefinition } from './data/sets/sinter'
import { SunionCommandDefinition } from './data/sets/sunion'
import { SmoveCommandDefinition } from './data/sets/smove'

// Sorted set commands
import { ZaddCommandDefinition } from './data/zsets/zadd'
import { ZremCommandDefinition } from './data/zsets/zrem'
import { ZrangeCommandDefinition } from './data/zsets/zrange'
import { ZrevrangeCommandDefinition } from './data/zsets/zrevrange'
import { ZrankCommandDefinition } from './data/zsets/zrank'
import { ZrevrankCommandDefinition } from './data/zsets/zrevrank'
import { ZscoreCommandDefinition } from './data/zsets/zscore'
import { ZcardCommandDefinition } from './data/zsets/zcard'
import { ZincrbyCommandDefinition } from './data/zsets/zincrby'
import { ZrangebyscoreCommandDefinition } from './data/zsets/zrangebyscore'
import { ZremrangebyscoreCommandDefinition } from './data/zsets/zremrangebyscore'
import { PingCommandDefinition } from './ping'
import { InfoCommandDefinition } from './info'
import { QuitCommandDefinition } from './quit'
import { MonitorCommandDefinition } from './monitor'
import { CommandInfoDefinition } from './command'
import { ClientCommandDefinition } from './client'
import { ClientSetNameCommandDefinition } from './client/clientSetName'
import { ScriptCommandDefinition } from './script'
import { ScriptLoadCommandDefinition } from './script/load'
import { ScriptExistsCommandDefinition } from './script/exists'
import { ScriptFlushCommandDefinition } from './script/flush'
import { ScriptKillCommandDefinition } from './script/kill'
import { ScriptDebugCommandDefinition } from './script/debug'
import { ScriptHelpCommandDefinition } from './script/help'
import { ClusterCommandDefinition } from './cluster'
import { ClusterInfoCommandDefinition } from './cluster/clusterInfo'
import { ClusterNodesCommandDefinition } from './cluster/clusterNodes'
import { ClusterSlotsCommandDefinition } from './cluster/clusterSlots'
import { ClusterShardsCommandDefinition } from './cluster/clusterShards'

export function createCommandRegistry(
  deps: CommandDependencies,
): CommandRegistry {
  const registry = new CommandRegistry()

  // String commands (13 commands)
  // Register commands that have been migrated to the new metadata system
  registry.registerAll([
    GetCommandDefinition,
    SetCommandDefinition,
    MgetCommandDefinition,
    MsetCommandDefinition,
    MsetnxCommandDefinition,
    GetsetCommandDefinition,
    AppendCommandDefinition,
    StrlenCommandDefinition,
    IncrCommandDefinition,
    DecrCommandDefinition,
    IncrbyCommandDefinition,
    DecrbyCommandDefinition,
    IncrbyfloatCommandDefinition,

    // Key commands (10 commands)
    DelCommandDefinition,
    ExistsCommandDefinition,
    TypeCommandDefinition,
    TtlCommandDefinition,
    PttlCommandDefinition,
    ExpireCommandDefinition,
    ExpireatCommandDefinition,
    FlushdbCommandDefinition,
    FlushallCommandDefinition,
    DbSizeCommandDefinition,

    // Hash commands (12 commands)
    HsetCommandDefinition,
    HgetCommandDefinition,
    HdelCommandDefinition,
    HgetallCommandDefinition,
    HmgetCommandDefinition,
    HmsetCommandDefinition,
    HkeysCommandDefinition,
    HvalsCommandDefinition,
    HlenCommandDefinition,
    HexistsCommandDefinition,
    HincrbyCommandDefinition,
    HincrbyfloatCommandDefinition,

    // List commands (10 commands)
    LpushCommandDefinition,
    RpushCommandDefinition,
    LpopCommandDefinition,
    RpopCommandDefinition,
    LlenCommandDefinition,
    LrangeCommandDefinition,
    LindexCommandDefinition,
    LsetCommandDefinition,
    LremCommandDefinition,
    LtrimCommandDefinition,

    // Set commands (11 commands)
    SaddCommandDefinition,
    SremCommandDefinition,
    ScardCommandDefinition,
    SmembersCommandDefinition,
    SismemberCommandDefinition,
    SpopCommandDefinition,
    SrandmemberCommandDefinition,
    SdiffCommandDefinition,
    SinterCommandDefinition,
    SunionCommandDefinition,
    SmoveCommandDefinition,

    // Sorted set commands (11 commands)
    ZaddCommandDefinition,
    ZremCommandDefinition,
    ZrangeCommandDefinition,
    ZrevrangeCommandDefinition,
    ZrankCommandDefinition,
    ZrevrankCommandDefinition,
    ZscoreCommandDefinition,
    ZcardCommandDefinition,
    ZincrbyCommandDefinition,
    ZrangebyscoreCommandDefinition,
    ZremrangebyscoreCommandDefinition,

    // Server/connection commands
    PingCommandDefinition,
    InfoCommandDefinition,
    QuitCommandDefinition,
    MonitorCommandDefinition,
    CommandInfoDefinition,
    ClientCommandDefinition,
    ClientSetNameCommandDefinition,

    // Script commands
    ScriptCommandDefinition,
    ScriptLoadCommandDefinition,
    ScriptExistsCommandDefinition,
    ScriptFlushCommandDefinition,
    ScriptKillCommandDefinition,
    ScriptDebugCommandDefinition,
    ScriptHelpCommandDefinition,
  ])

  if (deps.discoveryService && deps.mySelfId) {
    registry.registerAll([
      ClusterCommandDefinition,
      ClusterInfoCommandDefinition,
      ClusterNodesCommandDefinition,
      ClusterSlotsCommandDefinition,
      ClusterShardsCommandDefinition,
    ])
  }

  return registry
}
