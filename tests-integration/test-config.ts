import { Redis, Cluster } from 'ioredis'
import { createCluster, RedisClusterType } from 'redis'
import { buildRedisCluster, RedisCluster } from '../src/cluster'

export type TestBackend = 'mock' | 'real'

export class TestRunner {
  readonly backend = (process.env.TEST_BACKEND as TestBackend) || 'mock'
  private mockCluster: RedisCluster | null = null
  private ioredisCluster: Cluster[] = []
  private nodeRedisCluster: RedisClusterType[] = []

  private async ensureMockCluster(masters: number): Promise<RedisCluster> {
    if (!this.mockCluster) {
      this.mockCluster = buildRedisCluster({
        masters,
        replicasPerMaster: 0,
        basePort: 0,
      })
      await this.mockCluster.listen()
    }
    return this.mockCluster
  }

  async setupIoredisCluster(prefix?: string): Promise<Cluster> {
    if (this.backend === 'mock') {
      const mockCluster = await this.ensureMockCluster(3)

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
          slotsRefreshTimeout: 10000000,
          lazyConnect: true,
          keyPrefix: prefix,
          redisOptions: {
            commandTimeout: 10000000,
            connectTimeout: 100000,
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
      const mockCluster = await this.ensureMockCluster(1)

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
    if (this.mockCluster) {
      return this.mockCluster.nodes.map(node => node.port)
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
    await this.mockCluster?.close()
    this.mockCluster = null
  }

  getBackendName(): string {
    return this.backend === 'mock' ? 'Mock Redis Server' : 'Real Redis Server'
  }
}
