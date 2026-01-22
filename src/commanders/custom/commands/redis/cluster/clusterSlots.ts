import { CommandResult, DiscoveryService } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext, t } from '../../../schema'

export const commandName = 'slots'

export class ClusterSlotsCommand extends SchemaCommand<[]> {
  metadata = defineCommand(`cluster|${commandName}`, {
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

  protected schema = t.tuple([])

  protected execute(
    _args: [],
    { discoveryService, mySelfId, transport }: CommandContext,
  ) {
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
    transport.write(slots)
  }
}
