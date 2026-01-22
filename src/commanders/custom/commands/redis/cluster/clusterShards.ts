import { DiscoveryNode, DiscoveryService } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext, t } from '../../../schema'

export const commandName = 'shards'

export class ClusterShardsCommand extends SchemaCommand<[]> {
  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly mySelfId: string,
  ) {
    super()
  }

  metadata = defineCommand(`cluster|${commandName}`, {
    arity: 1, // CLUSTER SHARDS
    flags: {
      admin: true,
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CLUSTER],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], { transport }: CommandContext) {
    const me = this.discoveryService.getById(this.mySelfId)
    const mapping: Record<number, DiscoveryNode[]> = {}
    for (const clusterNode of this.discoveryService.getAll()) {
      const arr = (mapping[clusterNode.slots[0][0]] ??= [])
      if (this.discoveryService.isMaster(me.id)) arr.unshift(clusterNode)
      else arr.push(clusterNode)
    }
    const shards: [
      string,
      number[],
      string,
      [
        string,
        string,
        string,
        number,
        string,
        string,
        string,
        string,
        string,
        string,
        string,
        number,
        string,
        string,
      ][],
    ][] = []
    for (const clusterNodes of Object.values(mapping)) {
      const master = clusterNodes[0]
      const slots = master.slots.reduce<number[]>((acc, range) => {
        acc.push(...range)
        return acc
      }, [])
      shards.push([
        'slots',
        slots,
        'nodes',
        clusterNodes.map(clusterNode => {
          return [
            'id',
            clusterNode.id,
            'port',
            clusterNode.port,
            'ip',
            clusterNode.host,
            'endpoint',
            clusterNode.host,
            'role',
            master.id === clusterNode.id ? 'master' : 'replica',
            'replication-offset',
            1,
            'health',
            'online',
          ]
        }),
      ])
    }
    transport.write(shards)
  }
}
