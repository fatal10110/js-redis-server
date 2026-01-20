import { UnknwonClientSubCommand } from '../../../../../core/errors'
import { Command } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandContext,
  SchemaCommandRegistration,
  t,
} from '../../../schema'
import {
  ClientSetNameCommandDefinition,
  commandName as setNameCommandName,
} from './clientSetName'

export const commandName = 'client'

export class ClientCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer[]]>
{
  metadata = defineCommand(commandName, {
    arity: -2, // CLIENT <subcommand> [args...]
    flags: {
      readonly: true,
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  })

  schema = t.tuple([t.string(), t.variadic(t.string())])

  handler(
    [subCommandName, rest]: [Buffer, Buffer[]],
    ctx: SchemaCommandContext,
  ) {
    const subCommands = createSubCommands(ctx)
    const subCommand = subCommands[subCommandName.toString().toLowerCase()]
    if (!subCommand) {
      throw new UnknwonClientSubCommand(subCommandName.toString())
    }
    subCommand.run(subCommandName, rest, ctx.signal, ctx.transport)
  }
}

function createSubCommands(ctx: SchemaCommandContext): Record<string, Command> {
  const baseCtx = {
    db: ctx.db,
    discoveryService: ctx.discoveryService,
    mySelfId: ctx.mySelfId,
  }
  return {
    [setNameCommandName]: createSchemaCommand(
      new ClientSetNameCommandDefinition(),
      baseCtx,
    ),
  }
}

export default function (db: SchemaCommandContext['db']) {
  return createSchemaCommand(new ClientCommandDefinition(), { db })
}
