import { UnknwonClusterSubCommand } from '../../../../../core/errors'
import { Command } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandContext,
  SchemaCommandRegistration,
  t,
} from '../../../schema'
import {
  ClusterInfoCommandDefinition,
  commandName as clusterInfoCommandName,
} from './clusterInfo'
import {
  ClusterNodesCommandDefinition,
  commandName as clusterNodesCommandName,
} from './clusterNodes'
import {
  ClusterShardsCommandDefinition,
  commandName as clusterShardsCommandName,
} from './clusterShards'
import {
  ClusterSlotsCommandDefinition,
  commandName as clusterSlotsCommandName,
} from './clusterSlots'

const metadata = defineCommand('cluster', {
  arity: -2, // CLUSTER <subcommand> [args...]
  flags: {
    admin: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.CLUSTER],
})

export const ClusterCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.string(), t.variadic(t.string())]),
  handler: async ([subCommandName, rest], ctx) => {
    const subCommands = createSubCommands(ctx)
    const args = [subCommandName, ...rest]
    const subCommand = args.pop()

    if (!subCommand) {
      throw new UnknwonClusterSubCommand('')
    }

    const sub = subCommands[subCommand.toString().toLowerCase()]

    if (!sub) {
      throw new UnknwonClusterSubCommand(subCommand.toString())
    }

    return sub.run(subCommand, args, ctx.signal)
  },
}

function createSubCommands(ctx: SchemaCommandContext): Record<string, Command> {
  const baseCtx = {
    db: ctx.db,
    discoveryService: ctx.discoveryService,
    mySelfId: ctx.mySelfId,
  }

  return {
    [clusterInfoCommandName]: createSchemaCommand(
      ClusterInfoCommandDefinition,
      baseCtx,
    ),
    [clusterNodesCommandName]: createSchemaCommand(
      ClusterNodesCommandDefinition,
      baseCtx,
    ),
    [clusterShardsCommandName]: createSchemaCommand(
      ClusterShardsCommandDefinition,
      baseCtx,
    ),
    [clusterSlotsCommandName]: createSchemaCommand(
      ClusterSlotsCommandDefinition,
      baseCtx,
    ),
  }
}

export default function (db: SchemaCommandContext['db']) {
  return createSchemaCommand(ClusterCommandDefinition, { db })
}
