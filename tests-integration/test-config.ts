import { Redis, Cluster, type RedisOptions } from 'ioredis'
import {
  createClient,
  createCluster,
  RedisClientType,
  RedisClusterType,
} from 'redis'
import { spawn, ChildProcess } from 'node:child_process'
import { createServer, AddressInfo } from 'node:net'
import { setTimeout as delay } from 'node:timers/promises'
import type { Duplex } from 'node:stream'
import { createRedisCluster, RedisCluster } from '../src/cluster-server'
import {
  realClusterPorts,
  realStandaloneAuthPort,
  realStandalonePort,
  STANDALONE_AUTH_PASSWORD,
} from './redis-endpoints'
import {
  type CompatibilitySpec,
  Resp2Server,
  RedisServerState,
  createRedisCommandExecutor,
} from '../src/internal'
import {
  createIoredisMock,
  createNodeRedisMock,
  type NodeRedisMockClient,
  type NodeRedisMockCluster,
} from '../src/index'

/**
 * Which server the integration suites talk to:
 *
 *  - `mock` (default): in-process `Resp2Server` / `createRedisCluster` over TCP.
 *  - `real`: the docker-compose Redis cluster + standalone services.
 *  - `socketless`: the packaged socketless client mocks — `createIoredisMock`
 *    (the real ioredis client over a virtual socket) and `createNodeRedisMock`
 *    (the hand-written node-redis facade, `NodeRedisMockClient` /
 *    `NodeRedisMockCluster`). No port is ever bound: cluster node addresses
 *    are the mock's synthetic `host:port`s, reachable only over its virtual
 *    transport (see {@link openSocketlessStream}), and a setup that has to
 *    hand a test a real port (raw-tcp standalone, requirepass servers) throws
 *    {@link SocketlessUnsupportedError}. `REDIS_COMPAT` is ignored — the
 *    socketless factories take no profile. Divergences are listed in
 *    `tests-integration/socketless/known-gaps.ts` (#412).
 */
export type TestBackend = 'mock' | 'real' | 'socketless'

/**
 * Thrown by a `TestRunner` setup method the `socketless` backend cannot serve
 * — anything that needs a TCP port or a server option the socketless factories
 * do not take.
 */
export class SocketlessUnsupportedError extends Error {
  constructor(what: string) {
    super(`socketless backend cannot provide ${what}`)
    this.name = 'SocketlessUnsupportedError'
  }
}

/**
 * socketless: the in-memory `Connector` of the most recently set-up
 * `createIoredisMock({ cluster })` client. `connectToEndpoint()` (utils.ts)
 * hands it to the direct per-node `Redis` clients tests open onto a cluster
 * node's synthetic `host:port` — the same transport the mock cluster client
 * uses for its own node connections — since there is no socket to dial.
 */
let socketlessIoredisConnector: RedisOptions['Connector'] | undefined
let socketlessClusterPorts: number[] = []

/**
 * socketless: open a raw byte stream to a synthetic cluster node through the
 * mock's virtual transport, for `RawRedisConnection`. `undefined` on the TCP
 * backends (dial the port) or before a socketless cluster is set up.
 */
export async function openSocketlessStream(
  host: string,
  port: number,
): Promise<Duplex | undefined> {
  if (!socketlessIoredisConnector) {
    return undefined
  }
  const connector = new socketlessIoredisConnector({ host, port })
  return (await connector.connect(() => {})) as unknown as Duplex
}

/** Extra `Redis` options for a direct node connection (see above). */
export function directNodeRedisOptions(): Partial<RedisOptions> {
  return socketlessIoredisConnector
    ? {
        Connector: socketlessIoredisConnector,
        retryStrategy: () => null,
        maxRetriesPerRequest: 1,
      }
    : {}
}

/**
 * Password used by the password-protected standalone server (see
 * setupIoredisStandaloneAuth). Defined in `redis-endpoints.ts` and re-exported
 * here so the harness and `scripts/flush-redis.ts` cannot drift apart.
 */
export { STANDALONE_AUTH_PASSWORD }

export type IoredisClusterSetupOptions = {
  masters?: number
  replicasPerMaster?: number
}

export class TestRunner {
  readonly backend = (process.env.TEST_BACKEND as TestBackend) || 'mock'
  private readonly compatibility = process.env.REDIS_COMPAT as
    | CompatibilitySpec
    | undefined
  private readonly mockClusters = new Map<string, RedisCluster>()
  private activeMockCluster: RedisCluster | null = null
  private ioredisCluster: Cluster[] = []
  private nodeRedisCluster: RedisClusterType[] = []
  private standaloneServers: Resp2Server[] = []
  private standaloneProcs: ChildProcess[] = []
  private ioredisStandalone: Redis[] = []
  private nodeRedisStandalone: RedisClientType[] = []
  /**
   * socketless: one `createIoredisMock({ cluster })` root per cluster shape.
   * Tests get `duplicate()`s of it — same options, so the same in-memory
   * `Connector` and therefore the same keyspace — which mirrors the mock
   * backend handing every `setupIoredisCluster()` call a new client onto one
   * shared cluster. The root itself is only closed by `cleanup()`.
   */
  private readonly socketlessIoredisRoots = new Map<string, Cluster>()
  private readonly socketlessNodeRedisClusters = new Map<
    string,
    NodeRedisMockCluster
  >()
  private socketlessNodeRedisStandalone: NodeRedisMockClient[] = []

  private async ensureMockCluster(
    options: Required<IoredisClusterSetupOptions>,
  ): Promise<RedisCluster> {
    const key = mockClusterKey(options, this.compatibility)
    let cluster = this.mockClusters.get(key)
    if (!cluster) {
      cluster = createRedisCluster({
        masters: options.masters,
        replicasPerMaster: options.replicasPerMaster,
        basePort: 0,
        compatibility: this.compatibility,
      })
      this.mockClusters.set(key, cluster)
      await cluster.listen()
    }

    this.activeMockCluster = cluster
    return cluster
  }

  async setupIoredisCluster(
    prefix?: string,
    options: IoredisClusterSetupOptions = {},
  ): Promise<Cluster> {
    const clusterOptions = {
      masters: options.masters ?? 3,
      replicasPerMaster: options.replicasPerMaster ?? 0,
    }

    if (this.backend === 'socketless') {
      return this.setupSocketlessIoredisCluster(prefix, clusterOptions)
    }

    if (this.backend === 'mock') {
      const mockCluster = await this.ensureMockCluster(clusterOptions)

      const cluster = new Redis.Cluster(
        [
          {
            host: '127.0.0.1',
            port: mockCluster.nodes[0].port,
          },
        ],
        {
          slotsRefreshTimeout: 10000000,
          lazyConnect: true,
          keyPrefix: prefix,
        },
      )
      await cluster.connect()

      this.ioredisCluster.push(cluster)
      return cluster
    } else {
      const cluster = new Redis.Cluster(
        this.getClusterPorts().map(p => ({
          host: '127.0.0.1',
          port: p, // Real Redis cluster port
        })),
        {
          // Keep timeouts bounded so a misconfigured or half-formed cluster
          // fails fast instead of hanging the suite for hours.
          slotsRefreshTimeout: 10000,
          lazyConnect: true,
          keyPrefix: prefix,
          redisOptions: {
            commandTimeout: 10000,
            connectTimeout: 10000,
            offlineQueue: false,
            commandQueue: false,
          },
        },
      )
      await cluster.connect()

      this.ioredisCluster.push(cluster)
      return cluster
    }
  }

  async setupNodeRedisCluster(options: IoredisClusterSetupOptions = {}) {
    const clusterOptions = {
      masters: options.masters ?? 3,
      replicasPerMaster: options.replicasPerMaster ?? 0,
    }

    if (this.backend === 'socketless') {
      // The facade stands in for node-redis' RedisCluster; the cast is the
      // point — the suites drive it through node-redis' typed surface, and a
      // method the facade lacks fails the test that calls it.
      return (await this.setupSocketlessNodeRedisCluster(
        clusterOptions,
      )) as unknown as ReturnType<typeof createCluster>
    }

    if (this.backend === 'mock') {
      const mockCluster = await this.ensureMockCluster(clusterOptions)

      const redisClient = createCluster({
        rootNodes: mockCluster.nodes.map(node => ({
          url: `redis://127.0.0.1:${node.port}`,
        })),
      })
      await redisClient?.connect()

      this.nodeRedisCluster.push(redisClient as RedisClusterType)

      return redisClient
    } else {
      // Use real Redis
      const redisClient = createCluster({
        rootNodes: Array.from(this.getClusterPorts()).map(port => ({
          url: `redis://127.0.0.1:${port}`,
        })),
      })
      await redisClient?.connect()

      this.nodeRedisCluster.push(redisClient as RedisClusterType)

      return redisClient
    }
  }

  /**
   * Connect an ioredis client to a single standalone server (non-cluster).
   *
   * Standalone-only behavior — multiple logical databases and SELECT — cannot
   * be exercised through the cluster harness (cluster mode rejects SELECT), so
   * this path serves tests that need a real SELECT-capable server.
   *
   *  - mock: spin up an in-process Resp2Server with 16 databases
   *  - real: spawn a real `redis-server` child on a free port (also 16 DBs)
   */
  async setupIoredisStandalone(): Promise<Redis> {
    if (this.backend === 'socketless') {
      // A fresh 16-DB keyspace per call, like the mock backend's fresh
      // Resp2Server per call.
      const client = (await createIoredisMock()) as Redis
      this.ioredisStandalone.push(client)
      return client
    }

    const port =
      this.backend === 'mock'
        ? await this.startMockStandalone()
        : await this.startRealStandalone()

    const client = new Redis({ host: '127.0.0.1', port, lazyConnect: true })
    await client.connect()
    this.ioredisStandalone.push(client)
    return client
  }

  /**
   * node-redis equivalent of {@link setupIoredisStandalone}: connect a
   * node-redis client to a single standalone server (non-cluster) so
   * SELECT-capable / multi-database tests can run against node-redis too.
   *
   *  - mock: spin up an in-process Resp2Server with 16 databases
   *  - real: connect to REDIS_STANDALONE_PORT (docker-compose) or spawn a child
   */
  async setupNodeRedisStandalone(): Promise<RedisClientType> {
    if (this.backend === 'socketless') {
      const client = (await createNodeRedisMock()) as NodeRedisMockClient
      this.socketlessNodeRedisStandalone.push(client)
      return client as unknown as RedisClientType
    }

    const port =
      this.backend === 'mock'
        ? await this.startMockStandalone()
        : await this.startRealStandalone()

    const client = createClient({
      url: `redis://127.0.0.1:${port}`,
    }) as RedisClientType
    // Avoid unhandled 'error' events tearing down the test process.
    client.on('error', () => {})
    await client.connect()
    this.nodeRedisStandalone.push(client)
    return client
  }

  /**
   * node-redis equivalent of {@link setupIoredisStandaloneAuth}: connect a
   * node-redis client WITHOUT a password to a password-protected standalone
   * server so tests can drive AUTH / NOAUTH / WRONGPASS sequencing explicitly
   * via `client.sendCommand(['AUTH', STANDALONE_AUTH_PASSWORD])`.
   *
   * `disableClientInfo` skips node-redis' connect-time CLIENT SETINFO probes,
   * which would otherwise fail with NOAUTH on an unauthenticated connection.
   */
  async setupNodeRedisStandaloneAuth(): Promise<RedisClientType> {
    this.requireTcp('setupNodeRedisStandaloneAuth() (requirepass)')
    const port =
      this.backend === 'mock'
        ? await this.startMockStandaloneAuth()
        : await this.startRealStandaloneAuth()

    const client = createClient({
      url: `redis://127.0.0.1:${port}`,
      // Stay on RESP2 and skip the connect-time CLIENT SETINFO probes so the
      // client opens without sending HELLO/AUTH — the test drives AUTH itself.
      RESP: 2,
      disableClientInfo: true,
      socket: { reconnectStrategy: false },
    }) as RedisClientType
    // Swallow NOAUTH noise; the test drives AUTH manually.
    client.on('error', () => {})
    await client.connect()
    this.nodeRedisStandalone.push(client)
    return client
  }

  /**
   * Return a TCP port for a single standalone server, WITHOUT attaching any
   * client. Raw-TCP integration tests open their own bare socket against this
   * port to exercise wire-level behavior real clients can't produce (inline
   * commands, malformed frames, exact response bytes).
   *
   *  - mock: spin up an in-process Resp2Server with 16 databases
   *  - real: connect to REDIS_STANDALONE_PORT (docker-compose), or spawn a
   *    local redis-server child as a dev fallback
   */
  async setupRawStandalone(): Promise<number> {
    this.requireTcp('setupRawStandalone()')
    return this.backend === 'mock'
      ? this.startMockStandalone()
      : this.startRealStandalone()
  }

  /**
   * Start a password-protected (requirepass) standalone server for raw-tcp
   * tests and return its port. No client is created — callers drive the
   * AUTH/NOAUTH/WRONGPASS handshake over a bare `RawRedisConnection`.
   */
  async setupRawStandaloneAuth(): Promise<number> {
    this.requireTcp('setupRawStandaloneAuth()')
    return this.backend === 'mock'
      ? this.startMockStandaloneAuth()
      : this.startRealStandaloneAuth()
  }

  private async startMockStandalone(): Promise<number> {
    const state = new RedisServerState({
      databaseCount: 16,
      compatibility: this.compatibility,
    })
    const executor = createRedisCommandExecutor({
      compatibility: state.profile,
    })
    const server = new Resp2Server({ server: state, executor })
    await server.listen(0)
    this.standaloneServers.push(server)
    return server.getPort()
  }

  /**
   * Connect an ioredis client to a password-protected standalone server.
   *
   * The client is deliberately created WITHOUT a password so tests can drive
   * AUTH / NOAUTH / WRONGPASS sequencing explicitly. Authenticate from the test
   * with `client.call('AUTH', STANDALONE_AUTH_PASSWORD)`.
   *
   *  - mock: in-process Resp2Server configured with `requirepass`
   *  - real: connect to REDIS_STANDALONE_AUTH_PORT (docker-compose service), or
   *    spawn a local `redis-server --requirepass` child as a dev fallback
   */
  async setupIoredisStandaloneAuth(): Promise<Redis> {
    this.requireTcp('setupIoredisStandaloneAuth() (requirepass)')
    const port =
      this.backend === 'mock'
        ? await this.startMockStandaloneAuth()
        : await this.startRealStandaloneAuth()

    const client = new Redis({
      host: '127.0.0.1',
      port,
      lazyConnect: true,
      // Skip ioredis' INFO ready-check; an unauthenticated connection cannot
      // run it, and the test drives AUTH manually.
      enableReadyCheck: false,
      maxRetriesPerRequest: 1,
    })
    // Swallow NOAUTH noise from ioredis' own connect-time CLIENT SETINFO probes.
    client.on('error', () => {})
    await client.connect()
    this.ioredisStandalone.push(client)
    return client
  }

  private async startMockStandaloneAuth(): Promise<number> {
    const state = new RedisServerState({
      databaseCount: 16,
      requirepass: STANDALONE_AUTH_PASSWORD,
      compatibility: this.compatibility,
    })
    const executor = createRedisCommandExecutor({
      compatibility: state.profile,
    })
    const server = new Resp2Server({ server: state, executor })
    await server.listen(0)
    this.standaloneServers.push(server)
    return server.getPort()
  }

  private async startRealStandaloneAuth(): Promise<number> {
    const configuredPort = realStandaloneAuthPort()
    if (configuredPort !== undefined) {
      await waitForRedis(configuredPort, STANDALONE_AUTH_PASSWORD)
      return configuredPort
    }

    const port = await freePort()
    const proc = spawn(
      'redis-server',
      [
        '--port',
        String(port),
        '--requirepass',
        STANDALONE_AUTH_PASSWORD,
        '--save',
        '',
        '--appendonly',
        'no',
      ],
      { stdio: 'ignore' },
    )
    this.standaloneProcs.push(proc)
    await waitForRedis(port, STANDALONE_AUTH_PASSWORD)
    return port
  }

  private async startRealStandalone(): Promise<number> {
    // CI (and anyone running docker-compose.test.yml) provides a standalone
    // Redis whose host port is published via REDIS_STANDALONE_PORT — connect to
    // it instead of spawning, since the runner has no redis-server binary.
    const configuredPort = realStandalonePort()
    if (configuredPort !== undefined) {
      await waitForRedis(configuredPort)
      return configuredPort
    }

    // Local dev fallback: spawn our own redis-server child on a free port.
    const port = await freePort()
    const proc = spawn(
      'redis-server',
      ['--port', String(port), '--save', '', '--appendonly', 'no'],
      { stdio: 'ignore' },
    )
    this.standaloneProcs.push(proc)
    await waitForRedis(port)
    return port
  }

  /**
   * Bring up a cluster for raw-tcp tests and return its node ports.
   *
   * Unlike `setupIoredisCluster`, no client is created — callers open
   * `RawRedisConnection`s themselves (see `connectToRawSlotOwner`). This lets
   * wire-level tests that need cluster routing (raw MULTI/EXEC/WATCH, CROSSSLOT,
   * MOVED) run over a bare socket instead of through a client.
   */
  async setupRawCluster(
    options: IoredisClusterSetupOptions = {},
  ): Promise<number[]> {
    this.requireTcp('setupRawCluster()')
    if (this.backend === 'mock') {
      await this.ensureMockCluster({
        masters: options.masters ?? 3,
        replicasPerMaster: options.replicasPerMaster ?? 0,
      })
      return this.getMockClusterPorts()
    }
    return this.getRealClusterPorts()
  }

  getMockClusterPorts(): number[] {
    if (this.activeMockCluster) {
      return this.activeMockCluster.nodes.map(node => node.port)
    }
    return []
  }

  /**
   * Ports of the real cluster's nodes — docker-compose.test.yml's published
   * ports by default, overridable with REDIS_CLUSTER_PORTS so a developer can
   * point the suite at a private cluster instead of sharing one.
   *
   * Parsing lives in `redis-endpoints.ts` so `scripts/flush-redis.ts` resolves
   * the exact same list: a cleanup that flushes a different set of nodes than
   * the suite then uses would be silent in precisely the way #395 was.
   */
  getRealClusterPorts(): number[] {
    return realClusterPorts()
  }

  /**
   * socketless: the synthetic ports the in-memory cluster advertises in
   * CLUSTER SLOTS. Nothing listens on them — they resolve only through
   * `connectToEndpoint()` / `RawRedisConnection.connect()`, which route them
   * over the mock's own virtual transport (see {@link openSocketlessStream}).
   */
  getClusterPorts(): number[] {
    if (this.backend === 'socketless') {
      return socketlessClusterPorts
    }
    return this.backend === 'mock'
      ? this.getMockClusterPorts()
      : this.getRealClusterPorts()
  }

  async cleanup(): Promise<void> {
    // Clean up ioredis connections
    for (const cluster of this.ioredisCluster) {
      await cluster.disconnect()
      await cluster.quit()
    }

    // Clean up node-redis connection
    for (const cluster of this.nodeRedisCluster) {
      await cluster.close()
    }

    // Clean up standalone ioredis clients
    for (const client of this.ioredisStandalone) {
      client.disconnect()
    }
    this.ioredisStandalone = []

    // Clean up standalone node-redis clients
    for (const client of this.nodeRedisStandalone) {
      client.destroy()
    }
    this.nodeRedisStandalone = []

    // Clean up in-process standalone servers (mock backend)
    await Promise.all(this.standaloneServers.map(server => server.close()))
    this.standaloneServers = []

    // Kill spawned redis-server children (real backend)
    for (const proc of this.standaloneProcs) {
      proc.kill('SIGKILL')
    }
    this.standaloneProcs = []

    // Clean up mock cluster
    await Promise.all(
      Array.from(this.mockClusters.values()).map(cluster => cluster.close()),
    )
    this.mockClusters.clear()
    this.activeMockCluster = null

    // socketless: the ioredis roots own their in-memory cluster (quit() tears
    // it down); the node-redis facades own theirs too. A facade a test already
    // closed throws ClientClosedError on a second close — that is its contract
    // (see docs/TESTING.md), not a cleanup failure.
    await Promise.all(
      Array.from(this.socketlessIoredisRoots.values()).map(root =>
        root.quit().catch(() => undefined),
      ),
    )
    this.socketlessIoredisRoots.clear()
    socketlessIoredisConnector = undefined
    socketlessClusterPorts = []
    for (const cluster of this.socketlessNodeRedisClusters.values()) {
      cluster.destroy()
    }
    this.socketlessNodeRedisClusters.clear()
    for (const client of this.socketlessNodeRedisStandalone) {
      try {
        client.destroy()
      } catch {
        // already closed by the test
      }
    }
    this.socketlessNodeRedisStandalone = []
  }

  getBackendName(): string {
    if (this.backend === 'socketless') {
      return 'Socketless Client Mock'
    }
    return this.backend === 'mock' ? 'Mock Redis Server' : 'Real Redis Server'
  }

  /** Refuse, on the socketless backend, a setup that needs a TCP port. */
  private requireTcp(what: string): void {
    if (this.backend === 'socketless') {
      throw new SocketlessUnsupportedError(`${what} — it needs a TCP port`)
    }
  }

  private async setupSocketlessIoredisCluster(
    prefix: string | undefined,
    options: Required<IoredisClusterSetupOptions>,
  ): Promise<Cluster> {
    const key = mockClusterKey(options, undefined)
    let root = this.socketlessIoredisRoots.get(key)
    if (!root) {
      root = (await createIoredisMock({
        cluster: {
          masters: options.masters,
          replicasPerMaster: options.replicasPerMaster,
        },
      })) as Cluster
      this.socketlessIoredisRoots.set(key, root)
    }
    socketlessIoredisConnector = root.options.redisOptions?.Connector
    socketlessClusterPorts = root
      .nodes('all')
      .map(node => Number(node.options.port))
      .sort((a, b) => a - b)

    const cluster = root.duplicate([], { keyPrefix: prefix, lazyConnect: true })
    await cluster.connect()
    this.ioredisCluster.push(cluster)
    return cluster
  }

  /**
   * The node-redis cluster facade has no `duplicate()` and no way to open a
   * second client onto an existing `NodeRedisMockCluster`'s nodes, so unlike
   * the other backends a second `setupNodeRedisCluster()` in one file cannot
   * share the first one's keyspace. Handing back the same instance would put
   * both "clients" on one session per node (a blocked BLPOP would wedge the
   * other), so it throws instead and the file is listed in known-gaps.ts.
   */
  private async setupSocketlessNodeRedisCluster(
    options: Required<IoredisClusterSetupOptions>,
  ): Promise<NodeRedisMockCluster> {
    const key = mockClusterKey(options, undefined)
    if (this.socketlessNodeRedisClusters.has(key)) {
      throw new SocketlessUnsupportedError(
        'a second node-redis cluster client on the same keyspace (NodeRedisMockCluster has no duplicate())',
      )
    }
    const cluster = (await createNodeRedisMock({
      cluster: {
        masters: options.masters,
        replicas: options.replicasPerMaster,
      },
    })) as NodeRedisMockCluster
    this.socketlessNodeRedisClusters.set(key, cluster)
    return cluster
  }
}

function mockClusterKey(
  options: Required<IoredisClusterSetupOptions>,
  compatibility: CompatibilitySpec | undefined,
): string {
  return `${options.masters}:${options.replicasPerMaster}:${compatibility ?? 'default'}`
}

/** Grab an OS-assigned free TCP port (used to launch a real redis-server). */
function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer()
    srv.once('error', reject)
    srv.listen(0, () => {
      const port = (srv.address() as AddressInfo).port
      srv.close(() => resolve(port))
    })
  })
}

/** Poll a freshly spawned redis-server until it answers PING (or time out). */
async function waitForRedis(port: number, password?: string): Promise<void> {
  const deadline = Date.now() + 10000
  let lastError: unknown
  while (Date.now() < deadline) {
    const probe = new Redis({
      host: '127.0.0.1',
      port,
      password,
      lazyConnect: true,
      retryStrategy: () => null,
      maxRetriesPerRequest: 1,
    })
    // Swallow connection-refused noise while the server is still booting.
    probe.on('error', () => {})
    try {
      await probe.connect()
      const pong = await probe.ping()
      probe.disconnect()
      if (pong === 'PONG') {
        return
      }
    } catch (err) {
      lastError = err
      probe.disconnect()
      await delay(100)
    }
  }
  throw new Error(
    `standalone redis-server on ${port} did not become ready: ${String(lastError)}`,
  )
}
