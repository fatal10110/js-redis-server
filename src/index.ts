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
  createValkeyMock,
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

// Cluster builder.
export { computeSlotRange, type RedisClusterOptions } from './cluster'
export {
  RedisCluster,
  createRedisCluster,
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

// Client-visible error classes: the base class plus the few subclasses code
// tells apart (`instanceof`) or that name a cluster / auth / transaction
// protocol error. Every other error reply is a plain `RedisCommandError`.
export {
  ExecCommandAbortError,
  NoAuthError,
  RedisClusterDownError,
  RedisCommandError,
  RedisCrossSlotError,
  RedisMovedError,
  UnknownRedisCommandError,
  UnknownSubcommandError,
  WrongNumberOfArgumentsError,
  WrongTypeRedisError,
} from './core/redis-error'
