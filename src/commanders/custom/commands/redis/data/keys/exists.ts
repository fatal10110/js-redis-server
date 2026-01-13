import { WrongNumberOfArguments } from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const ExistsCommandDefinition: CommandDefinition = {
  metadata: defineCommand('exists', {
    arity: -2, // EXISTS key [key ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: -1, // Last argument is a key
    keyStep: 1,
    categories: [CommandCategory.GENERIC],
  }),
  factory: deps => new ExistsCommand(deps.db),
}

export class ExistsCommand implements Command {
  readonly metadata = ExistsCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length === 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length === 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    let count = 0
    for (let i = 0; i < args.length; i++) {
      if (this.db.get(args[i]) !== null) {
        count++
      }
    }

    return Promise.resolve({ response: count })
  }
}

export default function (db: DB) {
  return new ExistsCommand(db)
}
