import { WrongNumberOfArguments } from '../../../../core/errors'
import { Command, CommandResult } from '../../../../types'
import { defineCommand, CommandCategory } from '../metadata'
import type { CommandDefinition } from '../registry'

export const PingCommandDefinition: CommandDefinition = {
  metadata: defineCommand('ping', {
    arity: -1, // PING [message]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  }),
  factory: () => new Ping(),
}

export class Ping implements Command {
  readonly metadata = PingCommandDefinition.metadata

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

    return Promise.resolve({ response: 'PONG' })
  }
}

export default function () {
  return new Ping()
}
