import { WrongNumberOfArguments } from '../../../../../core/errors'
import {
  Command,
  CommandResult,
  DiscoveryNode,
  DiscoveryService,
} from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const commandName = 'slots'

export const ClusterSlotsCommandDefinition: CommandDefinition = {
  metadata: defineCommand(`cluster|${commandName}`, {
    arity: 1, // CLUSTER SLOTS
    flags: {
      admin: true,
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CLUSTER],
  }),
  factory: deps => {
    if (!deps.discoveryService || !deps.mySelfId) {
      throw new Error('Cluster slots requires discoveryService and mySelfId')
    }

    const me = deps.discoveryService.getById(deps.mySelfId)
    return new ClusterSlotsCommand(me, deps.discoveryService)
  },
}

export class ClusterSlotsCommand implements Command {
  readonly metadata = ClusterSlotsCommandDefinition.metadata

  constructor(
    private readonly me: DiscoveryNode,
    private readonly discoveryService: DiscoveryService,
  ) {}

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
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
