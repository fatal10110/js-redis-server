import { createRedisCommandExecutor } from '../commands'
import { buildClusterNodes } from '../cluster'
import {
  createVirtualConnection,
  type VirtualConnection,
} from '../core/transports/virtual-connection'
import { RedisServerState } from '../state'
import { InMemoryNodeRegistry, type NodePipeline } from './node-registry'
import type {
  Redis as RedisClient,
  Cluster as RedisCluster,
  RedisOptions,
} from 'ioredis'
import type AbstractConnector from 'ioredis/built/connectors/AbstractConnector'

const DEFAULT_HOST = '127.0.0.1'
const DEFAULT_DATABASE_COUNT = 16
const DEFAULT_CLUSTER_BASE_PORT = 7000

export type CreateIoredisMockOptions =
  | { cluster?: false; databaseCount?: number }
  | { cluster: { masters: number; replicasPerMaster?: number } }

/**
 * A drop-in, socketless `ioredis` client (standalone or `Cluster`) backed by the
 * in-memory server pipeline. The **real** ioredis client drives a fake
 * `net.Socket` ({@link createVirtualConnection}) over real RESP, so reply
 * shaping, pipelines, `multi`, pub/sub, and `scanStream` all work for free — no
 * TCP socket and no port bind.
 *
 * `ioredis` is an optional peer dependency, imported lazily, so the core stays
 * dependency-free.
 */
export async function createIoredisMock(
  options: CreateIoredisMockOptions = {},
): Promise<RedisClient | RedisCluster> {
  const ioredis = await loadIoredis()

  if ('cluster' in options && options.cluster) {
    return createClusterMock(ioredis, options.cluster)
  }

  const databaseCount =
    ('databaseCount' in options && options.databaseCount) ||
    DEFAULT_DATABASE_COUNT
  return createStandaloneMock(ioredis, databaseCount)
}

/**
 * The slice of the lazily-imported `ioredis` module this mock uses. Typed via
 * the public `ioredis` types so the dynamic `import('ioredis')` stays
 * dependency-free at the package boundary.
 */
type IoredisModule = {
  Redis: new (options: RedisOptions) => RedisClient
  Cluster: typeof import('ioredis').Cluster
  AbstractConnector: typeof AbstractConnector
}

async function loadIoredis(): Promise<IoredisModule> {
  try {
    const mod = (await import('ioredis')) as unknown as IoredisModule
    return mod
  } catch {
    throw new Error(
      "createIoredisMock requires the optional peer dependency 'ioredis'; install it to use the ioredis mock",
    )
  }
}

/**
 * The set of virtual connections opened by a mock client. ioredis opens one (or
 * a few) per node and may reconnect, so closing must drain every tracked
 * connection, not just the latest.
 */
class VirtualConnectionTracker {
  private readonly connections = new Set<VirtualConnection>()

  open(
    pipeline: NodePipeline,
    address: { host: string; port: number },
  ): VirtualConnection {
    const connection = createVirtualConnection({
      state: pipeline.state,
      executor: pipeline.executor,
      nodeRole: pipeline.nodeRole,
      remoteAddress: address.host,
      remotePort: address.port,
    })
    this.connections.add(connection)
    void connection.done.finally(() => this.connections.delete(connection))
    return connection
  }

  async closeAll(): Promise<void> {
    const draining = [...this.connections].map(connection => {
      connection.close()
      return connection.done
    })
    this.connections.clear()
    await Promise.allSettled(draining)
  }
}

/**
 * Build the `Connector` class ioredis instantiates (`new Connector(options)`).
 * It resolves its own `options.host/port` against the registry and opens a
 * virtual connection to that node's in-memory pipeline. Closing over the
 * registry + tracker means `duplicate()` (same `Connector` reference) reuses the
 * same pipelines — the two clients share one keyspace + pub/sub broker.
 */
function buildConnectorClass(
  ioredis: IoredisModule,
  registry: InMemoryNodeRegistry,
  tracker: VirtualConnectionTracker,
) {
  type ConnectorOptions = {
    disconnectTimeout?: number
    host?: string
    port?: number
  }

  return class InMemoryConnector extends ioredis.AbstractConnector {
    private readonly host: string
    private readonly port: number

    constructor(options: unknown) {
      const opts = (options ?? {}) as ConnectorOptions
      super(opts.disconnectTimeout ?? 0)
      this.host = opts.host ?? DEFAULT_HOST
      this.port = opts.port ?? registry.nodes()[0]?.port ?? 0
    }

    connect(): Promise<AbstractConnector['stream']> {
      this.connecting = true
      const pipeline = registry.resolve(this.host, this.port)
      if (!pipeline) {
        return Promise.reject(
          new Error(
            `no in-memory node registered for ${this.host}:${this.port}`,
          ),
        )
      }

      const { clientSocket } = tracker.open(pipeline, {
        host: this.host,
        port: this.port,
      })
      this.stream = clientSocket as unknown as AbstractConnector['stream']
      return Promise.resolve(this.stream)
    }
  }
}

function createStandaloneMock(
  ioredis: IoredisModule,
  databaseCount: number,
): RedisClient {
  const state = new RedisServerState({ databaseCount })
  const executor = createRedisCommandExecutor()

  const registry = new InMemoryNodeRegistry()
  registry.register(DEFAULT_HOST, 6379, { state, executor })

  const tracker = new VirtualConnectionTracker()
  const Connector = buildConnectorClass(ioredis, registry, tracker)

  const redis = new ioredis.Redis({
    host: DEFAULT_HOST,
    port: 6379,
    Connector,
    lazyConnect: false,
    // No real socket = no real reconnect; a failed (post-close) connect must
    // not retry forever and keep the process alive.
    retryStrategy: () => null,
    maxRetriesPerRequest: 1,
  })

  attachTeardown(redis, () =>
    Promise.all([tracker.closeAll(), Promise.resolve(state.close())]),
  )
  return redis
}

function createClusterMock(
  ioredis: IoredisModule,
  options: { masters: number; replicasPerMaster?: number },
): RedisCluster {
  const built = buildClusterNodes({
    masters: options.masters,
    replicasPerMaster: options.replicasPerMaster ?? 0,
    basePort: DEFAULT_CLUSTER_BASE_PORT,
  })

  const registry = new InMemoryNodeRegistry()
  for (const node of built.nodes) {
    registry.register(node.host, node.port, {
      state: node.state,
      executor: node.executor,
      nodeRole: node.role,
    })
  }

  const tracker = new VirtualConnectionTracker()
  const Connector = buildConnectorClass(ioredis, registry, tracker)

  const firstNode = built.nodes[0]
  const cluster = new ioredis.Cluster(
    [{ host: firstNode.host, port: firstNode.port }],
    {
      lazyConnect: false,
      // A bounded refresh timeout keeps a misconfigured cluster from hanging.
      slotsRefreshTimeout: 10000,
      redisOptions: {
        Connector,
        maxRetriesPerRequest: 1,
      },
      // No real sockets, so neither the cluster nor a lost node should keep
      // retrying — a closed mock must let the process exit.
      clusterRetryStrategy: () => null,
      clusterNodeRetryStrategy: () => null,
    },
  )

  attachTeardown(cluster, async () => {
    await tracker.closeAll()
    for (const link of built.replicationLinks) {
      link.close()
    }
    for (const node of built.nodes) {
      node.state.close()
    }
  })
  return cluster
}

/**
 * Drain the in-memory pipelines whenever the client is closed. Both `quit()` and
 * `disconnect()` are wrapped so the virtual connections and server state are torn
 * down no matter which path the consumer (or ioredis itself) takes.
 */
function attachTeardown(
  client: RedisClient | RedisCluster,
  teardown: () => Promise<unknown>,
): void {
  let torn = false
  const runTeardown = async () => {
    if (torn) {
      return
    }
    torn = true
    await teardown()
  }

  const originalQuit = client.quit.bind(client)
  const originalDisconnect = client.disconnect.bind(client)

  client.quit = (async (...args: unknown[]) => {
    const reply = await (originalQuit as (...a: unknown[]) => Promise<unknown>)(
      ...args,
    )
    await runTeardown()
    return reply
  }) as typeof client.quit

  client.disconnect = ((...args: unknown[]) => {
    ;(originalDisconnect as (...a: unknown[]) => void)(...args)
    void runTeardown()
  }) as typeof client.disconnect
}
