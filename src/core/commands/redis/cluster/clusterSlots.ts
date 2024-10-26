import { Command, CommandResult } from '../../../../types'
import { ClusterNode } from '../../../cluster/clusterNode'

export const commandName = 'slots'

export class ClusterSlotsCommand implements Command {
  constructor(private readonly node: ClusterNode) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): CommandResult {
    const slots: unknown[] = []

    for (const clusterNode of this.node.getClusterNodes()) {
      if (clusterNode.masterNodeId) continue

      const address = clusterNode.getAddress()

      const nodeInfo: (string | number | Iterable<void>)[] = [
        address.host,
        address.port,
        clusterNode.id,
        [],
      ]
      slots.push([
        clusterNode.slotRange?.min,
        clusterNode.slotRange?.max,
        nodeInfo,
      ])
    }

    return { response: slots }
  }
}
