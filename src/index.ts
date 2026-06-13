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
export type { Logger } from './logger'
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
  ExpectedFloatError,
  ExpectedIntegerError,
  DiscardWithoutMultiError,
  ExecWithoutMultiError,
  InvalidExpireTimeError,
  MinMaxNotFloatError,
  NoScriptError,
  OffsetOutOfRangeError,
  PositiveCountError,
  RedisClusterDownError,
  RedisCrossSlotError,
  RedisCommandError,
  RedisMovedError,
  ScriptDebugModeError,
  ScriptCallNoCommandError,
  ScriptFlushOptionError,
  ScriptUnknownCommandError,
  RedisSyntaxError,
  TransactionDiscardedError,
  UnknownClusterSubcommandError,
  UnknownRedisCommandError,
  UnknownScriptSubcommandError,
  WatchInsideMultiError,
  WrongNumberOfKeysError,
  WrongNumberOfArgumentsError,
  WrongTypeRedisError,
  IndexOutOfRangeError,
  NoSuchKeyError,
  HashValueNotIntegerError,
  HashValueNotFloatError,
  NoAuthError,
  NoPasswordConfiguredError,
  WrongPassError,
  ZaddGtLtNxConflictError,
  ZaddIncrPairError,
  ZaddNxXxConflictError,
} from './core/redis-error'
export {
  createAuthPolicy,
  createClusterPolicy,
  createTransactionPolicy,
} from './core/execution-policies'
export {
  createDefaultParkHandler,
  createNoopParkHandler,
} from './core/redis-context'
export {
  RedisLuaRuntime,
  createRedisLuaRuntime,
  getDefaultRedisLuaRuntime,
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
  RedisKeyspace,
  RedisMutationBus,
  RedisPubSubBroker,
  RedisScriptCache,
  RedisServerState,
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
  createClusterCommand,
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
  persistCommand,
  pexpireCommand,
  pingCommand,
  pttlCommand,
  quitCommand,
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

// Transport
export {
  Resp2CommandDecoder,
  Resp2ParseError,
  Resp2SessionAdapter,
} from './core/transports/resp2'
export {
  Resp2Server,
  type Resp2ServerOptions,
} from './core/transports/resp2/server'
export { InMemoryConnectionTransport } from './core/transports/in-memory-connection-transport'
export { SocketConnectionTransport } from './core/transports/socket-connection-transport'

// Cluster builder (new core)
export {
  RedisCluster,
  buildRedisCluster,
  computeSlotRange,
  type RedisClusterNodeHandle,
  type RedisClusterOptions,
} from './cluster'
