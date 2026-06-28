import { Resp2Server } from './core/transports/resp2/server'
import { RedisClusterTopology, RedisServerState } from './state'
import {
  buildClusterNodes,
  type RedisClusterOptions,
  type ReplicationLink,
} from './cluster'

export type RedisClusterNodeHandle = {
  id: string
  role: 'master' | 'replica'
  host: string
  port: number
  server: RedisServerState
}

/**
 * A cluster of socket-backed nodes: {@link buildClusterNodes} assembles the
 * TCP-free pipelines, this wraps each in a {@link Resp2Server} and owns their
 * listen/close lifecycle. The socket layer of {@link buildClusterNodes} — kept
 * out of `cluster.ts` so the node-assembly there stays free of `net`.
 */
export class RedisCluster {
  readonly topology: RedisClusterTopology
  readonly nodes: readonly RedisClusterNodeHandle[]

  private readonly servers: Resp2Server[]
  private readonly replicationLinks: ReplicationLink[]

  constructor(
    topology: RedisClusterTopology,
    nodes: readonly RedisClusterNodeHandle[],
    servers: readonly Resp2Server[],
    replicationLinks: readonly ReplicationLink[],
  ) {
    this.topology = topology
    this.nodes = nodes
    this.servers = [...servers]
    this.replicationLinks = [...replicationLinks]
  }

  async listen(): Promise<void> {
    try {
      for (let index = 0; index < this.servers.length; index++) {
        await this.servers[index].listen(this.nodes[index].port)
      }
    } catch (err) {
      await this.close()
      throw err
    }

    // Backfill actual ports — when basePort is 0 the OS assigns random ports
    // and the handles/topology need to reflect what was actually bound.
    this.servers.forEach((server, index) => {
      const actualPort = server.getPort()
      ;(this.nodes[index] as { port: number }).port = actualPort
      ;(this.topology.nodes[index] as { port: number }).port = actualPort
    })
  }

  async close(): Promise<void> {
    for (const link of this.replicationLinks) {
      link.close()
    }
    const results = await Promise.allSettled(
      this.servers.map(server => server.close()),
    )
    for (const result of results) {
      if (result.status === 'fulfilled') {
        continue
      }
      if (isServerNotRunningError(result.reason)) {
        continue
      }
      throw result.reason
    }
  }

  getAddresses(): string[] {
    return this.nodes.map(node => `${node.host}:${node.port}`)
  }
}

function isServerNotRunningError(err: unknown): boolean {
  return (err as { code?: string }).code === 'ERR_SERVER_NOT_RUNNING'
}

/**
 * Builds an **un-started** cluster — call {@link RedisCluster.listen} yourself.
 *
 * @deprecated Prefer {@link createRedisServer} with the `cluster` option, which
 * builds *and* starts the cluster in one call (and is symmetric with
 * `createRedisMock({ cluster })`). This low-level builder remains for callers
 * that need to control when `listen()` runs.
 */
export function createRedisCluster(options: RedisClusterOptions): RedisCluster {
  const { topology, nodes, replicationLinks } = buildClusterNodes(options)

  const handles: RedisClusterNodeHandle[] = []
  const servers: Resp2Server[] = []

  for (const node of nodes) {
    const server = new Resp2Server({
      server: node.state,
      executor: node.executor,
      logger: options.logger,
      nodeRole: node.role,
    })

    handles.push({
      id: node.id,
      role: node.role,
      host: node.host,
      port: node.port,
      server: node.state,
    })
    servers.push(server)
  }

  return new RedisCluster(topology, handles, servers, replicationLinks)
}

/**
 * @deprecated Renamed to {@link createRedisCluster} for naming consistency with
 * `createRedisServer` / `createRedisMock`. This alias will be removed in a
 * future release.
 */
export const buildRedisCluster = createRedisCluster
