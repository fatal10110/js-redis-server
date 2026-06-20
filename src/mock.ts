import { createRedisCommandExecutor } from './commands'
import {
  buildRedisCluster,
  type RedisCluster,
  type RedisClusterNodeHandle,
} from './cluster'
import { Resp2Server } from './core/transports/resp2/server'
import { RedisServerState } from './state'
import { seedCluster, seedStandalone, type SeedEntry } from './seed'
import type { Logger } from './logger'

const DEFAULT_HOST = '127.0.0.1'
const DEFAULT_DATABASE_COUNT = 16

export type RedisAddress = { host: string; port: number }

export type CreateRedisServerOptions = {
  /** Port to bind. Defaults to `0` (OS-assigned free port). */
  port?: number
  /** Logical database count. Defaults to 16, matching real Redis. */
  databaseCount?: number
  logger?: Pick<Logger, 'error'>
}

export type RedisServerHandle = {
  host: string
  port: number
  state: RedisServerState
  server: Resp2Server
  close(): Promise<void>
}

/**
 * Standalone analog to {@link buildRedisCluster}: wires a {@link RedisServerState}
 * (16 databases by default), the shared command executor, and a
 * {@link Resp2Server}, then starts listening. Returns the live handle.
 */
export async function createRedisServer(
  options: CreateRedisServerOptions = {},
): Promise<RedisServerHandle> {
  const state = new RedisServerState({
    databaseCount: options.databaseCount ?? DEFAULT_DATABASE_COUNT,
  })
  const executor = createRedisCommandExecutor()
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
  logger?: Pick<Logger, 'error'>
}

/**
 * Friendly test-mock facade over a standalone server or a cluster. Exposes
 * connection helpers, seeding, reset-between-tests, and escape hatches to the
 * underlying state/nodes for power users.
 */
export interface RedisMock {
  readonly host: string
  readonly port: number
  readonly url: string
  /** ioredis-shaped single-node connection options. */
  connectionOptions(): RedisAddress
  /** Seed-node list for ioredis `Cluster` (single entry for standalone mocks). */
  clusterNodes(): RedisAddress[]
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

export async function createRedisMock(
  options: CreateRedisMockOptions = {},
): Promise<RedisMock> {
  if (options.cluster) {
    return createClusterMock(options.cluster, options)
  }
  return createStandaloneMock(options)
}

async function createStandaloneMock(
  options: CreateRedisMockOptions,
): Promise<RedisMock> {
  const handle = await createRedisServer({
    port: options.port,
    databaseCount: options.databaseCount,
    logger: options.logger,
  })

  const address: RedisAddress = { host: handle.host, port: handle.port }

  return {
    host: handle.host,
    port: handle.port,
    url: `redis://${handle.host}:${handle.port}`,
    connectionOptions: () => ({ ...address }),
    clusterNodes: () => [{ ...address }],
    seed: entries => seedStandalone(handle.state, entries),
    flush: async () => handle.state.flushAllDatabases(),
    reset: async () => handle.state.flushAllDatabases(),
    close: () => handle.close(),
    state: handle.state,
  }
}

async function createClusterMock(
  cluster: RedisMockClusterOptions,
  options: CreateRedisMockOptions,
): Promise<RedisMock> {
  const built = buildRedisCluster({
    masters: cluster.masters,
    replicasPerMaster: cluster.replicas ?? 0,
    basePort: options.basePort ?? 0,
    logger: options.logger,
  })

  await built.listen()

  const first = built.nodes[0]

  return {
    host: first.host,
    port: first.port,
    url: `redis://${first.host}:${first.port}`,
    connectionOptions: () => ({ host: first.host, port: first.port }),
    clusterNodes: () =>
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
