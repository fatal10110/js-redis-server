import { Redis, Cluster } from 'ioredis'
import { createCluster, RedisClusterType } from 'redis'
import { ClusterNetwork } from '../src/core/cluster/network'

export type TestBackend = 'mock' | 'real'

export class TestRunner {
  readonly backend = (process.env.TEST_BACKEND as TestBackend) || 'real'
  private mockCluster: ClusterNetwork | null = null
  private ioredisCluster: Cluster[] = []
  private nodeRedisCluster: RedisClusterType[] = []

  async setupIoredisCluster(prefix?: string): Promise<Cluster> {
    if (this.backend === 'mock') {
      // Use mock server
      if (!this.mockCluster) {
        this.mockCluster = new ClusterNetwork(console)
        await this.mockCluster.init({ masters: 3, slaves: 0 })
      }

      const cluster = new Redis.Cluster(
        [
          {
            host: '127.0.0.1',
            port: Array.from(this.mockCluster.getAll())[0].port,
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
      // Use mock server
      if (!this.mockCluster) {
        this.mockCluster = new ClusterNetwork(console)
        await this.mockCluster.init({ masters: 1, slaves: 0 })
      }

      const redisClient = createCluster({
        rootNodes: Array.from(this.mockCluster.getAll())
          .map(node => node.port)
          .map(port => ({
            url: `redis://127.0.0.1:${port}`,
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
      return Array.from(this.mockCluster.getAll()).map(node => node.port)
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
    await this.mockCluster?.shutdown()
  }

  getBackendName(): string {
    return this.backend === 'mock' ? 'Mock Redis Server' : 'Real Redis Server'
  }
}
