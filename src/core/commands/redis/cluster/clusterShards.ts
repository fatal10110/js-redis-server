import { Command, CommandResult } from '../../../../types'
import { ClusterNode } from '../../../cluster/clusterNode'

export const commandName = 'shards'

export class ClusterShardsCommand implements Command {
  constructor(private readonly node: ClusterNode) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): CommandResult {
    const mapping: Record<number, ClusterNode[]> = {}

    for (const clusterNode of this.node.getClusterNodes()) {
      const arr = (mapping[clusterNode.slotRange!.min] ??= [])

      if (this.node.masterNodeId) arr.push(clusterNode)
      else arr.unshift(clusterNode)
    }

    const shards = []

    for (const clusterNodes of Object.values(mapping)) {
      const master = clusterNodes[0]
      shards.push([
        'slots',
        [master.slotRange?.min, master.slotRange?.max],
        'nodes',
        clusterNodes.map(clusterNode => {
          const address = clusterNode.getAddress()

          return [
            'id',
            clusterNode.id,
            'port',
            address.port,
            'ip',
            address.host,
            'endpoint',
            address.host,
            'role',
            clusterNode.masterNodeId ? 'replica' : 'master',
            'replication-offset',
            1,
            'health',
            'online',
          ]
        }),
      ])
    }

    return { response: shards }
  }
}
