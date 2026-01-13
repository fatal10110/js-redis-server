import {
  UnknwonClientSubCommand,
  WrongNumberOfArguments,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'
import {
  ClientSetNameCommand,
  commandName as setNameCommandName,
} from './clientSetName'

export const commandName = 'client'

export const ClientCommandDefinition: CommandDefinition = {
  metadata: defineCommand(commandName, {
    arity: -2, // CLIENT <subcommand> [args...]
    flags: {
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  }),
  factory: () => new ClientCommand(createSubCommands()),
}

export class ClientCommand implements Command {
  readonly metadata = ClientCommandDefinition.metadata

  constructor(private readonly subCommands: Record<string, Command>) {}

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (!args.length) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(
    rawCommand: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<CommandResult> {
    const subCommandName = args.shift()

    if (!subCommandName) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const subComamnd = this.subCommands[subCommandName.toString().toLowerCase()]

    if (!subComamnd) {
      throw new UnknwonClientSubCommand(subCommandName.toString())
    }

    return subComamnd.run(subCommandName, args, signal)
  }
}

function createSubCommands(): Record<string, Command> {
  return {
    [setNameCommandName]: new ClientSetNameCommand(),
  }
}

export default function () {
  return new ClientCommand(createSubCommands())
}
