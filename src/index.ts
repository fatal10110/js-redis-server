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
  type RedisMockClusterOptions,
  type RedisServerHandle,
} from './mock'
export { seedCluster, seedStandalone, type SeedEntry } from './seed'

// In-memory drop-in client mocks (socketless real client over the in-memory
// pipeline). `ioredis` is an optional peer dependency, imported lazily.
export {
  createIoredisMock,
  type CreateIoredisMockOptions,
} from './client-mocks/ioredis-mock'
export {
  InMemoryRedis,
  InMemoryRedisClient,
  createInMemoryRedis,
  createInMemoryClient,
  type ConnectOptions,
  type CreateInMemoryRedisOptions,
  type CreateInMemoryClientOptions,
  type InMemoryRedisClientOptions,
  type RedisCommandArgument,
  type RedisNativeReply,
} from './in-memory-client'

// In-memory node-redis-shaped client mock (drop-in facade — no TCP socket).
export {
  createNodeRedisMock,
  NodeRedisMockClient,
  NodeRedisMockCluster,
  NodeRedisMockMulti,
  type CreateNodeRedisMockOptions,
  type NodeRedisMockClusterOptions,
  type NodeRedisCommandArgument,
  type NodeRedisReply,
  type NodeRedisZMember,
  type NodeRedisPubSubListener,
} from './client-mocks/node-redis-mock'

// Cluster builder (consistent `create*` naming; `buildRedisCluster` is a
// deprecated alias kept for back-compat).
export { computeSlotRange, type RedisClusterOptions } from './cluster'
export {
  RedisCluster,
  createRedisCluster,
  buildRedisCluster,
  type RedisClusterNodeHandle,
} from './cluster-server'

export type { Logger } from './logger'
export type {
  CompatibilityProfile,
  CompatibilitySpec,
  FeatureId,
  RedisFlavor,
  VersionGate,
} from './core/compatibility'
export {
  gateSatisfied,
  resolveCompatibilityProfile,
} from './core/compatibility'

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
