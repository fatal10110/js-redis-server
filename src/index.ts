// Curated public surface.
//
// Deep internals (command definitions, schema parsing, policies, transports,
// Lua, data-type helpers, …) live in the `js-redis-server/core` subpath and are
// re-exported here so importing from the root stays non-breaking. The symbols
// listed explicitly below are the promoted, first-class entry points: the
// test-mock facade, the server/cluster builders, and the client-visible error
// classes.
export * from './internal'

// Test-mock facade
export {
  createRedisMock,
  createRedisServer,
  type CreateRedisMockOptions,
  type CreateRedisServerOptions,
  type RedisAddress,
  type RedisMock,
  type RedisMockClientOptions,
  type RedisMockClusterOptions,
  type RedisMockTransport,
  type RedisServerHandle,
} from './mock'
export { seedCluster, seedStandalone, type SeedEntry } from './seed'
export {
  InMemoryRedisClient,
  createInMemoryClient,
  type InMemoryRedisClientOptions,
  type RedisCommandArgument,
  type RedisNativeReply,
} from './in-memory-client'

// Server / cluster builders
export { RedisServerState } from './state'
export { createRedisCommandExecutor } from './commands'
export {
  Resp2Server,
  type Resp2ServerOptions,
} from './core/transports/resp2/server'
export {
  RedisCluster,
  buildRedisCluster,
  computeSlotRange,
  type RedisClusterNodeHandle,
  type RedisClusterOptions,
} from './cluster'

export type { Logger } from './logger'

// Client-visible error classes
export {
  CountGreaterThanZeroError,
  ExpectedFloatError,
  ExpectedIntegerError,
  DiscardWithoutMultiError,
  ExecWithoutMultiError,
  InvalidExpireTimeError,
  LimitCantBeNegativeError,
  MinMaxNotFloatError,
  NoScriptError,
  NumKeysGreaterThanZeroError,
  OffsetOutOfRangeError,
  PositiveCountError,
  RedisClusterDownError,
  RedisCrossSlotError,
  RedisCommandError,
  RedisMovedError,
  ResultingScoreNaNError,
  ScriptDebugModeError,
  ScriptCallNoCommandError,
  ScriptFlushOptionError,
  ScriptUnknownCommandError,
  StreamElementTooLargeError,
  StreamIdExhaustedError,
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
