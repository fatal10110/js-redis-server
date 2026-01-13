import { WrongNumberOfArguments } from '../../../../core/errors'
import { Command, CommandResult } from '../../../../types'
import { defineCommand, CommandCategory } from '../metadata'
import type { CommandDefinition } from '../registry'

export const InfoCommandDefinition: CommandDefinition = {
  metadata: defineCommand('info', {
    arity: -1, // INFO [section]
    flags: {
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SERVER],
  }),
  factory: () => new InfoCommand(),
}

export class InfoCommand implements Command {
  readonly metadata = InfoCommandDefinition.metadata

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length > 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(_rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length > 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return Promise.resolve({ response: 'mock info' })
  }
}

export default function () {
  return new InfoCommand()
}
