import { Redis, Cluster } from 'ioredis'
import { createCluster, RedisClusterType } from 'redis'
import { buildRedisCluster, RedisCluster } from '../src/cluster'

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
