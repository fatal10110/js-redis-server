import { Redis, Cluster } from 'ioredis'
import { createCluster, RedisClusterType } from 'redis'
import { spawn, ChildProcess } from 'node:child_process'
import { createServer, AddressInfo } from 'node:net'
import { setTimeout as delay } from 'node:timers/promises'
import { buildRedisCluster, RedisCluster } from '../src/cluster'
import {
  Resp2Server,
  RedisServerState,
  createRedisCommandExecutor,
} from '../src'

export type TestBackend = 'mock' | 'real'

export type IoredisClusterSetupOptions = {
  masters?: number
  replicasPerMaster?: number
}

export class TestRunner {
  readonly backend = (process.env.TEST_BACKEND as TestBackend) || 'mock'
  private readonly mockClusters = new Map<string, RedisCluster>()
  private activeMockCluster: RedisCluster | null = null
  private ioredisCluster: Cluster[] = []
  private nodeRedisCluster: RedisClusterType[] = []
  private standaloneServers: Resp2Server[] = []
  private standaloneProcs: ChildProcess[] = []
  private ioredisStandalone: Redis[] = []

  private async ensureMockCluster(
    options: Required<IoredisClusterSetupOptions>,
  ): Promise<RedisCluster> {
    const key = mockClusterKey(options)
    let cluster = this.mockClusters.get(key)
    if (!cluster) {
      cluster = buildRedisCluster({
        masters: options.masters,
        replicasPerMaster: options.replicasPerMaster,
        basePort: 0,
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

  async setupNodeRedisCluster() {
    if (this.backend === 'mock') {
      const mockCluster = await this.ensureMockCluster({
        masters: 1,
        replicasPerMaster: 0,
      })

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
    const port =
      this.backend === 'mock'
        ? await this.startMockStandalone()
        : await this.startRealStandalone()

    const client = new Redis({ host: '127.0.0.1', port, lazyConnect: true })
    await client.connect()
    this.ioredisStandalone.push(client)
    return client
  }

  private async startMockStandalone(): Promise<number> {
    const state = new RedisServerState({ databaseCount: 16 })
    const executor = createRedisCommandExecutor()
    const server = new Resp2Server({ server: state, executor })
    await server.listen(0)
    this.standaloneServers.push(server)
    return server.getPort()
  }

  private async startRealStandalone(): Promise<number> {
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

  getMockClusterPorts(): number[] {
    if (this.activeMockCluster) {
      return this.activeMockCluster.nodes.map(node => node.port)
    }
    return []
  }

  getRealClusterPorts(): number[] {
    return [30000, 30001, 30002, 30003, 30004, 30005]
  }

  getClusterPorts(): number[] {
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
  }

  getBackendName(): string {
    return this.backend === 'mock' ? 'Mock Redis Server' : 'Real Redis Server'
  }
}

function mockClusterKey(options: Required<IoredisClusterSetupOptions>): string {
  return `${options.masters}:${options.replicasPerMaster}`
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
async function waitForRedis(port: number): Promise<void> {
  const deadline = Date.now() + 10000
  let lastError: unknown
  while (Date.now() < deadline) {
    const probe = new Redis({
      host: '127.0.0.1',
      port,
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
