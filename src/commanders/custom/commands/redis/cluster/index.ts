import { UnknwonClusterSubCommand } from '../../../../../core/errors'
import { Command } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext, t } from '../../../schema'
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

const subCommands: Record<string, Command> = {
  [clusterInfoCommandName]: new ClusterInfoCommand(),
  [clusterNodesCommandName]: new ClusterNodesCommand(),
  [clusterShardsCommandName]: new ClusterShardsCommand(),
  [clusterSlotsCommandName]: new ClusterSlotsCommand(),
}

export class ClusterCommand extends SchemaCommand<[Buffer, Buffer[]]> {
  metadata = defineCommand('cluster', {
    arity: -2, // CLUSTER <subcommand> [args...]
    flags: {
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CLUSTER],
  })

  protected schema = t.tuple([t.string(), t.variadic(t.string())])

  protected execute(
    [subCommandName, rest]: [Buffer, Buffer[]],
    ctx: CommandContext,
  ) {
    const args = [subCommandName, ...rest]
    const subCommand = args.pop()
    if (!subCommand) {
      throw new UnknwonClusterSubCommand('')
    }
    const sub = subCommands[subCommand.toString().toLowerCase()]
    if (!sub) {
      throw new UnknwonClusterSubCommand(subCommand.toString())
    }
    sub.run(subCommand, args, ctx)
  }
}
