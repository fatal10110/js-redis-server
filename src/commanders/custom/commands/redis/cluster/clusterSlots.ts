import { CommandResult, DiscoveryService } from '../../../../../types'
import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'

export const commandName = 'slots'

const metadata = defineCommand(`cluster|${commandName}`, {
  arity: 1, // CLUSTER SLOTS
  flags: {
    admin: true,
    readonly: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.CLUSTER],
})

export const ClusterSlotsCommandDefinition: SchemaCommandRegistration<[]> = {
  metadata,
  schema: t.tuple([]),
  handler: (_args, { discoveryService, mySelfId }) => {
    const service = discoveryService as DiscoveryService | undefined
    if (!service || !mySelfId) {
      throw new Error('Cluster slots requires discoveryService and mySelfId')
    }

    const slots: CommandResult[] = []

    for (const clusterNode of service.getAll()) {
      if (!service.isMaster(clusterNode.id)) continue

      const nodeInfo: CommandResult[] = [
        clusterNode.host,
        clusterNode.port,
        clusterNode.id,
        [],
      ]

      for (const [min, max] of clusterNode.slots) {
        slots.push([min, max, nodeInfo])
      }
    }

    return slots
  },
}

export default function (
  db: DB,
  discoveryService: DiscoveryService,
  mySelfId: string,
) {
  return createSchemaCommand(ClusterSlotsCommandDefinition, {
    db,
    discoveryService,
    mySelfId,
  })
}
