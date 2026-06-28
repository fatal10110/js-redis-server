import { createRedisCommandExecutor } from './commands'
import {
  createRedisCluster,
  type RedisCluster,
  type RedisClusterNodeHandle,
} from './cluster-server'
import type { CommandExecutor } from './core/command-executor'
import { Resp2Server } from './core/transports/resp2/server'
import { RedisServerState } from './state'
import { seedCluster, seedStandalone, type SeedEntry } from './seed'
import type { Logger } from './logger'
import {
  resolveCompatibilityProfile,
  type CompatibilitySpec,
} from './core/compatibility'

const DEFAULT_HOST = '127.0.0.1'
const DEFAULT_DATABASE_COUNT = 16

export type RedisAddress = { host: string; port: number }

export type CreateRedisServerOptions = {
  /** Port to bind. Defaults to `0` (OS-assigned free port). */
  port?: number
  /** Logical database count. Defaults to 16, matching real Redis. */
  databaseCount?: number
  compatibility?: CompatibilitySpec
  logger?: Pick<Logger, 'error'>
}

export type CreateRedisServerClusterOptions = {
  /** Build a (listening) cluster instead of a standalone server. */
  cluster: RedisMockClusterOptions
  /** Cluster base port. Defaults to `0` (each node OS-assigned). */
  basePort?: number
  /** Databases per cluster node. Defaults to 1. */
  databasesPerNode?: number
  compatibility?: CompatibilitySpec
  logger?: Pick<Logger, 'error'>
}

export type RedisServerHandle = {
  host: string
  port: number
  state: RedisServerState
  server: Resp2Server
  close(): Promise<void>
}

/** Build a standalone keyspace + command pipeline (no transport). */
function createStandalonePipeline(
  databaseCount?: number,
  compatibility?: CompatibilitySpec,
): {
  state: RedisServerState
  executor: CommandExecutor
} {
  const profile = resolveCompatibilityProfile(compatibility)
  const state = new RedisServerState({
    databaseCount: databaseCount ?? DEFAULT_DATABASE_COUNT,
    compatibility: profile,
  })
  return {
    state,
    executor: createRedisCommandExecutor({ compatibility: profile }),
  }
}

/**
 * Runs a real, **listening** Redis server you connect a normal client library
 * to. Standalone by default (one {@link RedisServerState} + executor +
 * {@link Resp2Server}, 16 databases); pass `cluster` to build and start a whole
 * cluster instead — the returned {@link RedisCluster} is already listening.
 *
 * For tests prefer {@link createRedisMock}, which wraps this and adds seeding,
 * reset-between-tests, and an in-process socketless client. Drop to
 * `js-redis-server/core` only when you need to assemble the pipeline by hand.
 *
 * @see {@link createRedisMock} — the test-mock facade (use this for test suites)
 */
export function createRedisServer(
  options?: CreateRedisServerOptions,
): Promise<RedisServerHandle>
export function createRedisServer(
  options: CreateRedisServerClusterOptions,
): Promise<RedisCluster>
export async function createRedisServer(
  options: CreateRedisServerOptions | CreateRedisServerClusterOptions = {},
): Promise<RedisServerHandle | RedisCluster> {
  if ('cluster' in options) {
    const cluster = createRedisCluster({
      masters: options.cluster.masters,
      replicasPerMaster: options.cluster.replicas ?? 0,
      basePort: options.basePort ?? 0,
      databasesPerNode: options.databasesPerNode,
      compatibility: options.compatibility,
      logger: options.logger,
    })
    await cluster.listen()
    return cluster
  }

  const { state, executor } = createStandalonePipeline(
    options.databaseCount,
    options.compatibility,
  )
  const server = new Resp2Server({
    server: state,
    executor,
    logger: options.logger,
  })

  await server.listen(options.port ?? 0)

  return {
    host: DEFAULT_HOST,
    port: server.getPort(),
    state,
    server,
    close: () => server.close(),
  }
}

export type RedisMockClusterOptions = {
  masters: number
  replicas?: number
}

export type CreateRedisMockOptions = {
  /** When set, builds a cluster mock instead of a standalone one. */
  cluster?: RedisMockClusterOptions
  /** Standalone-only: logical database count (defaults to 16). */
  databaseCount?: number
  /** Standalone bind port. Defaults to `0` (OS-assigned). */
  port?: number
  /** Cluster base port. Defaults to `0` (each node OS-assigned). */
  basePort?: number
  compatibility?: CompatibilitySpec
  logger?: Pick<Logger, 'error'>
}

/**
 * Friendly test-mock facade over a standalone server or a cluster. Exposes
 * connection helpers, seeding, reset-between-tests, and escape hatches to the
 * underlying state/nodes for power users.
 *
 * Need a socketless in-process client (no TCP, no RESP)? Use the standalone
 * {@link createInMemoryClient} builder instead.
 */
export interface RedisMock {
  readonly host: string
  readonly port: number
  readonly url: string
  /**
   * Node addresses as `{ host, port }[]` — a single entry for standalone mocks,
   * every node for cluster mocks. Client-agnostic: index `[0]` for a single
   * client, or pass the whole array to a cluster client.
   *
   * @example new Redis(mock.addresses()[0])         // ioredis, standalone
   * @example new Redis.Cluster(mock.addresses())    // ioredis, cluster
   */
  addresses(): RedisAddress[]
  seed(entries: readonly SeedEntry[]): Promise<void>
  flush(): Promise<void>
  /** Alias for {@link RedisMock.flush}. */
  reset(): Promise<void>
  close(): Promise<void>
  /** Escape hatch: the underlying state (standalone mocks only). */
  readonly state?: RedisServerState
  /** Escape hatch: the underlying node handles (cluster mocks only). */
  readonly nodes?: readonly RedisClusterNodeHandle[]
}

/**
 * Creates a {@link RedisMock} — the entry point for test suites. Standalone by
 * default; pass `{ cluster: { masters } }` for a cluster mock. Adds seeding and
 * reset-between-tests on top of a real server/cluster.
 *
 * To run a long-running server a separate process connects to (a CLI, a dev
 * tool) rather than drive it from test code, use {@link createRedisServer}
 * instead (it takes the same `cluster` option).
 *
 * @see {@link createRedisServer} — run a real server/cluster without test helpers
 */
export async function createRedisMock(
  options: CreateRedisMockOptions = {},
): Promise<RedisMock> {
  if (options.cluster) {
    return createClusterMock(options.cluster, options)
  }
  return createTcpStandaloneMock(options)
}

async function createTcpStandaloneMock(
  options: CreateRedisMockOptions,
): Promise<RedisMock> {
  const { state, executor } = createStandalonePipeline(
    options.databaseCount,
    options.compatibility,
  )
  const server = new Resp2Server({
    server: state,
    executor,
    logger: options.logger,
  })
  await server.listen(options.port ?? 0)

  const address: RedisAddress = { host: DEFAULT_HOST, port: server.getPort() }

  return {
    host: address.host,
    port: address.port,
    url: `redis://${address.host}:${address.port}`,
    addresses: () => [{ ...address }],
    seed: entries => seedStandalone(state, entries),
    flush: async () => state.flushAllDatabases(),
    reset: async () => state.flushAllDatabases(),
    close: async () => {
      await server.close()
    },
    state,
  }
}

async function createClusterMock(
  cluster: RedisMockClusterOptions,
  options: CreateRedisMockOptions,
): Promise<RedisMock> {
  const built = createRedisCluster({
    masters: cluster.masters,
    replicasPerMaster: cluster.replicas ?? 0,
    basePort: options.basePort ?? 0,
    compatibility: options.compatibility,
    logger: options.logger,
  })

  await built.listen()

  const first = built.nodes[0]

  return {
    host: first.host,
    port: first.port,
    url: `redis://${first.host}:${first.port}`,
    addresses: () =>
      built.nodes.map(node => ({ host: node.host, port: node.port })),
    seed: entries => seedCluster(built, entries),
    flush: () => flushCluster(built),
    reset: () => flushCluster(built),
    close: () => built.close(),
    nodes: built.nodes,
  }
}

async function flushCluster(cluster: RedisCluster): Promise<void> {
  for (const node of cluster.nodes) {
    node.server.flushAllDatabases()
  }
}
