import { Command, CommandResult } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const CommandInfoDefinition: CommandDefinition = {
  metadata: defineCommand('command', {
    arity: -1, // COMMAND [subcommand]
    flags: {
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SERVER],
  }),
  factory: () => new CommandsInfo(),
}

export class CommandsInfo implements Command {
  readonly metadata = CommandInfoDefinition.metadata

  getKeys(_rawCmd: Buffer, _args: Buffer[]): Buffer[] {
    return []
  }

  run(): Promise<CommandResult> {
    return Promise.resolve({ response: 'mock response' }) // TODO
  }
}

export default function () {
  return new CommandsInfo()
}
