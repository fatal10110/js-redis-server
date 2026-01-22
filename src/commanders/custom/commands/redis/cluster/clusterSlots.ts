import { CommandResult, DiscoveryService } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext, t } from '../../../schema'

export const commandName = 'slots'

export class ClusterSlotsCommand extends SchemaCommand<[]> {
  constructor(private readonly discoveryService: DiscoveryService) {
    super()
  }

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

  protected execute(_args: [], { transport }: CommandContext) {
    const slots: CommandResult[] = []
    for (const clusterNode of this.discoveryService.getAll()) {
      if (!this.discoveryService.isMaster(clusterNode.id)) continue
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
