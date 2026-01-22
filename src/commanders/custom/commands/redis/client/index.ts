import { UnknwonClientSubCommand } from '../../../../../core/errors'
import { Command } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext, t } from '../../../schema'
import {
  ClientSetNameCommand,
  commandName as setNameCommandName,
} from './clientSetName'

export const commandName = 'client'

const subCommands: Record<string, Command> = {
  [setNameCommandName]: new ClientSetNameCommand(),
}

export class ClientCommand extends SchemaCommand<[Buffer, Buffer[]]> {
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

  protected schema = t.tuple([t.string(), t.variadic(t.string())])

  protected execute(
    [subCommandName, rest]: [Buffer, Buffer[]],
    ctx: CommandContext,
  ) {
    const subCommand = subCommands[subCommandName.toString().toLowerCase()]
    if (!subCommand) {
      throw new UnknwonClientSubCommand(subCommandName.toString())
    }
    subCommand.run(subCommandName, rest, ctx)
  }
}
