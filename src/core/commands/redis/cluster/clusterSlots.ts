import { NodeCommand } from '..'
import { DiscoveryService } from '../../../cluster/network'
import { Node } from '../../../node'

export class ClusterSlots implements NodeCommand {
  handle(
    discoveryService: DiscoveryService,
    node: Node,
    args: unknown[],
  ): Iterable<unknown> {
    const slots: unknown[] = []

    for (const {
      node,
      host,
      port,
    } of discoveryService.getNodesAndAddresses()) {
      const nodeInfo: (string | number | Iterable<void>)[] = [
        host,
        port,
        node.id,
        [],
      ]
      slots.push([node.slotRange?.min, node.slotRange?.max, nodeInfo])
    }

    return slots
  }
}

export default new ClusterSlots()
