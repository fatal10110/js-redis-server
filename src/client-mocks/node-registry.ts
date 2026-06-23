import type { CommandExecutor } from '../core/command-executor'
import type { RedisClusterNodeRole, RedisServerState } from '../state'

/**
 * The in-memory pipeline behind one synthetic `host:port`: the node's keyspace
 * state, its command executor (cluster-aware for cluster nodes), and its role.
 */
export type NodePipeline = {
  state: RedisServerState
  executor: CommandExecutor
  nodeRole?: RedisClusterNodeRole
}

export type RegisteredNode = NodePipeline & {
  host: string
  port: number
}

/**
 * Maps a synthetic `host:port` to its in-memory {@link NodePipeline} so a client
 * library's per-node connections resolve to the right state/executor without any
 * TCP socket. A standalone mock registers a single entry; a cluster mock
 * registers one entry per node (the synthetic ports advertised by CLUSTER SLOTS).
 */
export class InMemoryNodeRegistry {
  private readonly nodesByAddress = new Map<string, RegisteredNode>()

  register(host: string, port: number, pipeline: NodePipeline): void {
    this.nodesByAddress.set(addressKey(host, port), {
      ...pipeline,
      host,
      port,
    })
  }

  resolve(host: string, port: number): NodePipeline | undefined {
    return this.nodesByAddress.get(addressKey(host, port))
  }

  nodes(): readonly RegisteredNode[] {
    return [...this.nodesByAddress.values()]
  }
}

function addressKey(host: string, port: number): string {
  return `${host}:${port}`
}
