import clusterKeySlot from 'cluster-key-slot'

export type RedisClusterNodeRole = 'master' | 'replica'

export type RedisClusterNode = {
  id: string
  role: RedisClusterNodeRole
  host: string
  port: number
  masterId?: string
  slots: Array<[number, number]>
}

export const REDIS_CLUSTER_SLOT_COUNT = 16384

export class RedisClusterTopology {
  constructor(public readonly nodes: readonly RedisClusterNode[] = []) {
    validateTopology(nodes)
  }

  calculateSlot(key: Buffer): number {
    return clusterKeySlot(key)
  }

  calculateSlotForKeys(keys: readonly Buffer[]): number | null {
    if (keys.length === 0) {
      return null
    }

    return clusterKeySlot.generateMulti([...keys])
  }

  getNode(id: string): RedisClusterNode | undefined {
    for (const node of this.nodes) {
      if (node.id === id) {
        return node
      }
    }

    return undefined
  }

  /**
   * Returns the master node owning the slot. Replicas are never returned —
   * MOVED must always direct the client to a master. Returns undefined if
   * the slot is unassigned.
   */
  getSlotOwner(slot: number): RedisClusterNode | undefined {
    for (const node of this.nodes) {
      if (node.role !== 'master') {
        continue
      }

      if (nodeOwnsSlot(node, slot)) {
        return node
      }
    }

    return undefined
  }

  nodeOwnsSlot(nodeId: string, slot: number): boolean {
    const node = this.getNode(nodeId)
    return node ? nodeOwnsSlot(node, slot) : false
  }
}

function validateTopology(nodes: readonly RedisClusterNode[]): void {
  const ids = new Set<string>()
  for (const node of nodes) {
    if (ids.has(node.id)) {
      throw new Error(`Duplicate cluster node id ${node.id}`)
    }
    ids.add(node.id)

    for (const [min, max] of node.slots) {
      if (
        !Number.isInteger(min) ||
        !Number.isInteger(max) ||
        min < 0 ||
        max >= REDIS_CLUSTER_SLOT_COUNT ||
        min > max
      ) {
        throw new Error(
          `Invalid slot range [${min}, ${max}] on node ${node.id}`,
        )
      }
    }

    if (node.role === 'replica' && !node.masterId) {
      throw new Error(`Replica node ${node.id} is missing masterId`)
    }
  }
}

function nodeOwnsSlot(node: RedisClusterNode, slot: number): boolean {
  for (const [min, max] of node.slots) {
    if (slot >= min && slot <= max) {
      return true
    }
  }

  return false
}
