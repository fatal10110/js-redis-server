import {
  UnknwonClusterSubCommand,
  WrongNumberOfArguments,
} from '../../../../../core/errors'
import { Command, CommandResult, DiscoveryService } from '../../../../../types'
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

export class ClusterCommand implements Command {
  constructor(private readonly subCommands: Record<string, Command>) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    const subCommandName = args.pop()

    if (!subCommandName) {
      throw new WrongNumberOfArguments(rawCmd.toString())
    }

    const subComamnd = this.subCommands[subCommandName.toString().toLowerCase()]

    if (!subComamnd) {
      throw new UnknwonClusterSubCommand(subCommandName.toString())
    }

    return subComamnd.run(subCommandName, args)
  }
}

export default function (discoveryService: DiscoveryService, mySelfId: string) {
  const me = discoveryService.getById(mySelfId)
  const subCommands = {
    [clusterInfoCommandName]: new ClusterInfoCommand(me, discoveryService),
    [clusterNodesCommandName]: new ClusterNodesCommand(me, discoveryService),
    [clusterShardsCommandName]: new ClusterShardsCommand(me, discoveryService),
    [clusterSlotsCommandName]: new ClusterSlotsCommand(me, discoveryService),
  }

  return new ClusterCommand(subCommands)
}
