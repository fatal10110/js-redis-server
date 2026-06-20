import { createRedisCommandExecutor } from './commands'
import {
  createRedisCluster,
  type RedisCluster,
  type RedisClusterNodeHandle,
} from './cluster'
import type { CommandExecutor } from './core/command-executor'
import { Resp2Server } from './core/transports/resp2/server'
import { RedisServerState } from './state'
import { seedCluster, seedStandalone, type SeedEntry } from './seed'
import {
  InMemoryRedisClient,
  type InMemoryRedisClientOptions,
} from './in-memory-client'
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

export type CreateRedisServerClusterOptions = {
  /** Build a (listening) cluster instead of a standalone server. */
  cluster: RedisMockClusterOptions
  /** Cluster base port. Defaults to `0` (each node OS-assigned). */
  basePort?: number
  /** Databases per cluster node. Defaults to 1. */
  databasesPerNode?: number
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
function createStandalonePipeline(databaseCount?: number): {
  state: RedisServerState
  executor: CommandExecutor
} {
  const state = new RedisServerState({
    databaseCount: databaseCount ?? DEFAULT_DATABASE_COUNT,
  })
  return { state, executor: createRedisCommandExecutor() }
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
      logger: options.logger,
    })
    await cluster.listen()
    return cluster
  }

  const { state, executor } = createStandalonePipeline(options.databaseCount)
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

/**
 * Transport backing a standalone mock:
 *  - `'tcp'` (default): a real loopback {@link Resp2Server} you connect a normal
 *    client library to (and may also drive via {@link RedisMock.client}).
 *  - `'memory'`: no TCP listener at all — only the socketless
 *    {@link RedisMock.client} works. The network accessors (`host`/`port`/`url`/
 *    `connectionOptions`/`clusterNodes`) throw.
 */
export type RedisMockTransport = 'tcp' | 'memory'

export type CreateRedisMockOptions = {
  /** When set, builds a cluster mock instead of a standalone one. */
  cluster?: RedisMockClusterOptions
  /** Standalone transport (defaults to `'tcp'`). Ignored for cluster mocks. */
  transport?: RedisMockTransport
  /** Standalone-only: logical database count (defaults to 16). */
  databaseCount?: number
  /** Standalone bind port (TCP transport). Defaults to `0` (OS-assigned). */
  port?: number
  /** Cluster base port. Defaults to `0` (each node OS-assigned). */
  basePort?: number
  logger?: Pick<Logger, 'error'>
}

export type RedisMockClientOptions = Pick<
  InMemoryRedisClientOptions,
  'database' | 'returnBuffers'
>

/**
 * Friendly test-mock facade over a standalone server or a cluster. Exposes
 * connection helpers, seeding, a socketless client, reset-between-tests, and
 * escape hatches to the underlying state/nodes for power users.
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
  /**
   * Socketless in-process client over the same command pipeline (no TCP, no
   * RESP). Standalone mocks only — cluster mocks throw; connect a real cluster
   * client to {@link RedisMock.clusterNodes} instead. Closed automatically when
   * the mock closes.
   */
  client(options?: RedisMockClientOptions): InMemoryRedisClient
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
 * default; pass `{ cluster: { masters } }` for a cluster mock. Adds seeding,
 * reset-between-tests, and an in-process socketless client on top of a real
 * server/cluster.
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
    if (options.transport === 'memory') {
      throw new Error(
        "the 'memory' transport is not supported for cluster mocks; use a tcp cluster mock",
      )
    }
    return createClusterMock(options.cluster, options)
  }
  if (options.transport === 'memory') {
    return createMemoryMock(options)
  }
  return createTcpStandaloneMock(options)
}

/** Track and lazily build socketless clients over a standalone pipeline. */
function createClientFactory(
  state: RedisServerState,
  executor: CommandExecutor,
): {
  client: (options?: RedisMockClientOptions) => InMemoryRedisClient
  closeAll: () => void
} {
  const clients = new Set<InMemoryRedisClient>()
  return {
    client: clientOptions => {
      const client = new InMemoryRedisClient({
        server: state,
        executor,
        ...clientOptions,
      })
      clients.add(client)
      return client
    },
    closeAll: () => {
      for (const client of clients) {
        client.close()
      }
      clients.clear()
    },
  }
}

async function createTcpStandaloneMock(
  options: CreateRedisMockOptions,
): Promise<RedisMock> {
  const { state, executor } = createStandalonePipeline(options.databaseCount)
  const server = new Resp2Server({
    server: state,
    executor,
    logger: options.logger,
  })
  await server.listen(options.port ?? 0)

  const address: RedisAddress = { host: DEFAULT_HOST, port: server.getPort() }
  const { client, closeAll } = createClientFactory(state, executor)

  return {
    host: address.host,
    port: address.port,
    url: `redis://${address.host}:${address.port}`,
    connectionOptions: () => ({ ...address }),
    clusterNodes: () => [{ ...address }],
    seed: entries => seedStandalone(state, entries),
    client,
    flush: async () => state.flushAllDatabases(),
    reset: async () => state.flushAllDatabases(),
    close: async () => {
      closeAll()
      await server.close()
    },
    state,
  }
}

function createMemoryMock(options: CreateRedisMockOptions): RedisMock {
  const { state, executor } = createStandalonePipeline(options.databaseCount)
  const { client, closeAll } = createClientFactory(state, executor)

  const noEndpoint = (): never => {
    throw new Error(
      'this mock uses the in-memory transport and has no TCP endpoint; use mock.client()',
    )
  }

  return {
    get host(): string {
      return noEndpoint()
    },
    get port(): number {
      return noEndpoint()
    },
    get url(): string {
      return noEndpoint()
    },
    connectionOptions: noEndpoint,
    clusterNodes: noEndpoint,
    seed: entries => seedStandalone(state, entries),
    client,
    flush: async () => state.flushAllDatabases(),
    reset: async () => state.flushAllDatabases(),
    close: async () => {
      closeAll()
      state.close()
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
    client: () => {
      throw new Error(
        'client() is not supported for cluster mocks; connect a real cluster client to clusterNodes() instead',
      )
    },
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
