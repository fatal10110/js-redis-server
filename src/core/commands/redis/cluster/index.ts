import { Command, CommandResult } from '../../../../types'
import { ClusterNode } from '../../../cluster/clusterNode'
import {
  UnknwonClusterSubCommand,
  WrongNumberOfArguments,
} from '../../../errors'
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

  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
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

export default function (node: ClusterNode) {
  const subCommands = {
    [clusterInfoCommandName]: new ClusterInfoCommand(node),
    [clusterNodesCommandName]: new ClusterNodesCommand(node),
    [clusterShardsCommandName]: new ClusterShardsCommand(node),
    [clusterSlotsCommandName]: new ClusterSlotsCommand(node),
  }

  return function () {
    return new ClusterCommand(subCommands)
  }
}
