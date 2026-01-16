import { DiscoveryNode, DiscoveryService } from '../../../../../types'
import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'

export const commandName = 'shards'

const metadata = defineCommand(`cluster|${commandName}`, {
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

export const ClusterShardsCommandDefinition: SchemaCommandRegistration<[]> = {
  metadata,
  schema: t.tuple([]),
  handler: (_args, { discoveryService, mySelfId }) => {
    const service = discoveryService as DiscoveryService | undefined
    if (!service || !mySelfId) {
      throw new Error('Cluster shards requires discoveryService and mySelfId')
    }

    const me = service.getById(mySelfId)
    const mapping: Record<number, DiscoveryNode[]> = {}

    for (const clusterNode of service.getAll()) {
      const arr = (mapping[clusterNode.slots[0][0]] ??= [])

      if (service.isMaster(me.id)) arr.unshift(clusterNode)
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

    return { response: shards }
  },
}

export default function (
  db: DB,
  discoveryService: DiscoveryService,
  mySelfId: string,
) {
  return createSchemaCommand(ClusterShardsCommandDefinition, {
    db,
    discoveryService,
    mySelfId,
  })
}
