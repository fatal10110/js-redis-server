import {
  UnknwonClusterSubCommand,
  WrongNumberOfArguments,
} from '../../../../../core/errors'
import { Command, CommandResult, DiscoveryService } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'
import {
  ClusterInfoCommand,
  commandName as clusterInfoCommandName,
} from './clusterInfo'
import {
  ClusterNodesCommand,
  commandName as clusterNodesCommandName,
} from './clusterNodes'
import {
  ClusterShardsCommand,
  commandName as clusterShardsCommandName,
} from './clusterShards'
import {
  ClusterSlotsCommand,
  commandName as clusterSlotsCommandName,
} from './clusterSlots'

export const ClusterCommandDefinition: CommandDefinition = {
  metadata: defineCommand('cluster', {
    arity: -2, // CLUSTER <subcommand> [args...]
    flags: {
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CLUSTER],
  }),
  factory: deps => {
    if (!deps.discoveryService || !deps.mySelfId) {
      throw new Error('Cluster command requires discoveryService and mySelfId')
    }

    return new ClusterCommand(
      createSubCommands(deps.discoveryService, deps.mySelfId),
    )
  },
}

export class ClusterCommand implements Command {
  readonly metadata = ClusterCommandDefinition.metadata

  constructor(private readonly subCommands: Record<string, Command>) {}

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (!args.length) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<CommandResult> {
    const subCommandName = args.pop()

    if (!subCommandName) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const subComamnd = this.subCommands[subCommandName.toString().toLowerCase()]

    if (!subComamnd) {
      throw new UnknwonClusterSubCommand(subCommandName.toString())
    }

    return subComamnd.run(subCommandName, args, signal)
  }
}

export default function (discoveryService: DiscoveryService, mySelfId: string) {
  return new ClusterCommand(createSubCommands(discoveryService, mySelfId))
}

function createSubCommands(
  discoveryService: DiscoveryService,
  mySelfId: string,
): Record<string, Command> {
  const me = discoveryService.getById(mySelfId)

  return {
    [clusterInfoCommandName]: new ClusterInfoCommand(me, discoveryService),
    [clusterNodesCommandName]: new ClusterNodesCommand(me, discoveryService),
    [clusterShardsCommandName]: new ClusterShardsCommand(me, discoveryService),
    [clusterSlotsCommandName]: new ClusterSlotsCommand(me, discoveryService),
  }
}
