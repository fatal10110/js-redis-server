import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const commandName = 'setname'

export const ClientSetNameCommandDefinition: CommandDefinition = {
  metadata: defineCommand(`client|${commandName}`, {
    arity: 2, // CLIENT SETNAME <name>
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  }),
  factory: () => new ClientSetNameCommand(),
}

export class ClientSetNameCommand implements Command {
  readonly metadata = ClientSetNameCommandDefinition.metadata

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(_rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return Promise.resolve({
      response: 'OK',
    })
  }
}
