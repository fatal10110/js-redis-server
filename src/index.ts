// Curated public surface — the small set of symbols a test/dev consumer needs:
// the test-mock facade, the `create*` server/cluster builders, seeding, the
// socketless client, and the client-visible error classes.
//
// Deep internals and hand-wiring building blocks (command definitions, schema
// parsing, execution policies, transports, `Resp2Server` / `RedisServerState` /
// `createRedisCommandExecutor`, Lua, data-type helpers, …) are intentionally
// NOT on the root. Import them from the `js-redis-server/core` subpath instead.

// Test-mock facade
export {
  createRedisMock,
  createRedisServer,
  type CreateRedisMockOptions,
  type CreateRedisServerOptions,
  type CreateRedisServerClusterOptions,
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

// Cluster builder (consistent `create*` naming; `buildRedisCluster` is a
// deprecated alias kept for back-compat).
export {
  RedisCluster,
  createRedisCluster,
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
