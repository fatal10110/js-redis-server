import { WrongNumberOfArguments } from '../../../../../core/errors'
import {
  Command,
  CommandResult,
  DiscoveryNode,
  DiscoveryService,
} from '../../../../../types'

export const commandName = 'slots'

export class ClusterSlotsCommand implements Command {
  constructor(
    private readonly me: DiscoveryNode,
    private readonly discoveryService: DiscoveryService,
  ) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(`cluster|${commandName}`)
    }

    const slots: unknown[] = []

    for (const clusterNode of this.discoveryService.getAll()) {
      if (!this.discoveryService.isMaster(clusterNode.id)) continue

      const nodeInfo: (string | number | Iterable<void>)[] = [
        clusterNode.host,
        clusterNode.port,
        clusterNode.id,
        [],
      ]

      for (const [min, max] of clusterNode.slots) {
        slots.push([min, max, nodeInfo])
      }
    }

    return Promise.resolve({ response: slots })
  }
}
