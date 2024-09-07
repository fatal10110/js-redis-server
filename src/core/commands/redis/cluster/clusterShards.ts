import { NodeCommand } from '..'
import { Discovery, DiscoveryService } from '../../../cluster/network'
import { Node } from '../../../node'

export class ClusterShards implements NodeCommand {
  handle(
    discoveryService: DiscoveryService,
    node: Node,
    args: unknown[],
  ): Iterable<unknown> {
    const mapping: Record<number, Discovery[]> = {}

    for (const discovery of discoveryService.getNodesAndAddresses()) {
      const arr = (mapping[discovery.node.slotRange!.min] ??= [])

      if (node.master) arr.push(discovery)
      else arr.unshift(discovery)
    }

    const shards = []

    for (const discoveries of Object.values(mapping)) {
      const master = discoveries[0].node
      shards.push([
        'slots',
        [master.slotRange?.min, master.slotRange?.max],
        'nodes',
        discoveries.map(discovery => [
          'id',
          discovery.node.id,
          'port',
          discovery.port,
          'ip',
          discovery.host,
          'endpoint',
          discovery.host,
          'role',
          discovery.node.master ? 'replica' : 'master',
          'replication-offset',
          1,
          'health',
          'online',
        ]),
      ])
    }

    return shards
  }
}

export default new ClusterShards()
