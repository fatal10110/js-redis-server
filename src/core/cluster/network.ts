import { Logger } from '../../types'
import { createClusterCommandsInputBuilder } from '../commands/redis'
import { DB } from '../db'
import { ClusterNode, RedisClusterNode, SlotRange } from './clusterNode'

const slots = 16384

export class ClusterNetwork {
  private readonly cluster: Map<string, ClusterNode> = new Map()
  private commandsShutdown?: () => void

  constructor(private readonly logger: Logger) {}

  getById(id: string) {
    const clusterNode = this.cluster.get(id)

    if (!clusterNode) {
      throw new Error(`Unknown node id ${id}`)
    }

    return clusterNode
  }

  getAll(): IterableIterator<ClusterNode> {
    return this.cluster.values()
  }

  getBySlot(slot: number): ClusterNode {
    for (const node of this.cluster.values()) {
      if (!node.slotRange) {
        throw new Error(`Missing slot range on node ${node.id}`)
      }

      if (node.slotRange.max >= slot && node.slotRange.min <= slot) {
        return node
      }
    }

    throw new Error(`unknown slot ${slot}`)
  }

  async init(params: { masters: number; slaves: number }) {
    const db = new DB()
    const [shutdown, createCommandInput] =
      await createClusterCommandsInputBuilder()
    // TODO temporary solution, find a better solution
    this.commandsShutdown = shutdown
    for (let i = 0; i < params.masters; i++) {
      const slotRange: SlotRange = {
        min: Math.round((slots * i) / params.masters),
        max: Math.round((slots * (i + 1)) / params.masters) - 1,
      }
      const node = new RedisClusterNode(
        this.logger,
        db,
        slotRange,
        this,
        createCommandInput,
      )
      this.cluster.set(node.id, node)

      for (let j = 0; j < params.slaves; j++) {
        const replica = new RedisClusterNode(
          this.logger,
          db,
          slotRange,
          this,
          createCommandInput,
          node.id,
        )
        this.cluster.set(replica.id, replica)
      }
    }

    await Promise.all(
      Array.from(this.cluster.values()).map(node => node.listen()),
    )
  }

  async shutdown() {
    await Promise.all(
      Array.from(this.cluster.values()).map(node => node.close()),
    )

    this.commandsShutdown?.()
  }
}
