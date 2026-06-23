// Deep internals — building blocks for power users who assemble the pipeline by
// hand (custom commands, policies, transports, schema parsing, Lua, …).
//
// Published as the `js-redis-server/core` subpath. The package root
// (`js-redis-server`) intentionally exposes only the curated consumer facade;
// import from this subpath when you need these lower-level pieces.

export type {
  CommandCapabilities,
  CommandExecutionResult,
  CommandFlag,
  CommandDefinition,
  CommandPlan,
} from './core/command-definition'

export type {
  CommandSchema,
  InferSchema,
  ParseContext,
  ParseNodeResult,
} from './core/command-schema'

export type {
  RedisClientSession,
  ClientSessionMode,
  ParkHandler,
  ParkRequest,
  RedisExecutionContext,
} from './core/redis-context'

export type {
  ExpirationState,
  KeyspaceEntry,
  RedisClusterNode,
  RedisClusterNodeRole,
  RedisDataTypeName,
  RedisDataValue,
  RedisHashData,
  RedisHashField,
  RedisListData,
  RedisMonitorCommandEvent,
  RedisMonitorCommandListener,
  RedisMutationEvent,
  RedisMutationListener,
  RedisServerStateOptions,
  RedisSetData,
  RedisSortedSetData,
  RedisSortedSetMember,
  RedisStreamData,
  RedisStringData,
  SetOptions,
  Unsubscribe,
} from './state'

export type { RedisResultOptions } from './core/redis-result'
export type { RespEncodeOptions, RespVersion } from './core/resp-encoder'
export type { ResponseStream } from './core/response-stream'
export type { RedisTurnHandle, RedisTurnQueue } from './core/turn-queue'
export type { ClientSessionOptions } from './core/client-session'
export type {
  ConnectionTransport,
  ConnectionTransportEvent,
  ConnectionTransportListener,
  ConnectionTransportUnsubscribe,
} from './core/transports/connection-transport'
export type { SocketConnectionTransportOptions } from './core/transports/socket-connection-transport'
export type {
  Resp2CommandFrame,
  Resp2SessionAdapterOptions,
} from './core/transports/resp2'

export { ClientSession } from './core/client-session'
export { CommandExecutor, type ExecutorResult } from './core/command-executor'
export { CommandRegistry } from './core/command-registry'
export { defineCommand } from './core/command-definition'
export { t, parseCommandArgs } from './core/command-schema'
export {
  createAuthPolicy,
  createClusterPolicy,
  createTransactionPolicy,
} from './core/execution-policies'
export {
  createDefaultParkHandler,
  createNoopParkHandler,
  createNonBlockingParkHandler,
} from './core/redis-context'
export {
  RedisLuaRuntime,
  createRedisLuaRuntime,
  luaReplyToRedisValue,
  type LuaReplyValue,
} from './core/lua-runtime'
export { RedisValue } from './core/redis-value'
export { RedisResult } from './core/redis-result'
export { encodeRedisResult, encodeRedisValue } from './core/resp-encoder'
export { isResponseStream } from './core/response-stream'
export { SerialTurnQueue } from './core/turn-queue'
export {
  REDIS_CLUSTER_SLOT_COUNT,
  RedisClusterTopology,
  RedisDatabase,
  RedisServerState,
  RedisKeyspace,
  RedisMonitorFeed,
  RedisMutationBus,
  RedisPubSubBroker,
  RedisScriptCache,
  WrongRedisTypeError,
  cloneRedisDataValue,
  createHashData,
  createListData,
  createSetData,
  createSortedSetData,
  createStringData,
} from './state'
export {
  connectionCommands,
  createRedisCommandExecutor,
  createRedisCommandRegistry,
  commandCommand,
  copyCommand,
  createClusterCommands,
  dbsizeCommand,
  delCommand,
  discardCommand,
  existsCommand,
  evalCommand,
  evalshaCommand,
  execCommand,
  expireCommand,
  expiretimeCommand,
  pexpiretimeCommand,
  flushallCommand,
  flushdbCommand,
  getCommand,
  hscanCommand,
  keysCommand,
  keysCommands,
  mgetCommand,
  multiCommand,
  monitorCommand,
  monitorCommands,
  persistCommand,
  pexpireCommand,
  pingCommand,
  pttlCommand,
  quitCommand,
  randomkeyCommand,
  readonlyCommand,
  readwriteCommand,
  redisCommandDefinitions,
  scanCommand,
  scanCommands,
  selectCommand,
  setCommand,
  scriptCommand,
  scriptsCommands,
  sscanCommand,
  stringsCommands,
  transactionCommands,
  ttlCommand,
  typeCommand,
  unwatchCommand,
  watchCommand,
  zscanCommand,
} from './commands'

// Transport internals
export {
  Resp2CommandDecoder,
  Resp2ParseError,
  Resp2SessionAdapter,
} from './core/transports/resp2'
export { InMemoryConnectionTransport } from './core/transports/in-memory-connection-transport'
export { SocketConnectionTransport } from './core/transports/socket-connection-transport'

// Pipeline assembly building blocks — the pieces `createRedisMock` /
// `createRedisServer` / `createRedisCluster` wire together. Power users who
// assemble the pipeline by hand reach for these directly.
export {
  Resp2Server,
  type Resp2ServerOptions,
} from './core/transports/resp2/server'
export {
  RedisCluster,
  createRedisCluster,
  buildRedisCluster,
  computeSlotRange,
  type RedisClusterNodeHandle,
  type RedisClusterOptions,
} from './cluster'
// Client-agnostic in-memory cluster that routes each command to its slot owner
// up front (client-side routing — no MOVED round-trips).
export {
  InMemoryCluster,
  createInMemoryCluster,
  type InMemoryClusterOptions,
  type CommandArgument,
} from './client-mocks/in-memory-cluster'

// Client-visible error classes (also re-exported from the package root).
export * from './core/redis-error'
