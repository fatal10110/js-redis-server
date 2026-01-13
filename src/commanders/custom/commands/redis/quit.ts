import { WrongNumberOfArguments } from '../../../../core/errors'
import { Command, CommandResult } from '../../../../types'
import { defineCommand, CommandCategory } from '../metadata'
import type { CommandDefinition } from '../registry'

export const QuitCommandDefinition: CommandDefinition = {
  metadata: defineCommand('quit', {
    arity: 1, // QUIT
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  }),
  factory: () => new QuitCommand(),
}

export class QuitCommand implements Command {
  readonly metadata = QuitCommandDefinition.metadata

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(_rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return Promise.resolve({
      close: true,
      response: 'OK',
    })
  }
}

export default function () {
  return new QuitCommand()
}
