import { WrongNumberOfArguments } from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const DelCommandDefinition: CommandDefinition = {
  metadata: defineCommand('del', {
    arity: -2, // DEL key [key ...]
    flags: {
      write: true,
    },
    firstKey: 0,
    lastKey: -1, // Last argument is a key
    keyStep: 1,
    categories: [CommandCategory.GENERIC],
  }),
  factory: deps => new DelCommand(deps.db),
}

export class DelCommand implements Command {
  readonly metadata = DelCommandDefinition.metadata

  constructor(private readonly db: DB) {}
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (!args.length) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    let counter = 0

    for (const key of args) {
      if (this.db.del(key)) {
        counter++
      }
    }

    return Promise.resolve({ response: counter })
  }
}

export default function (db: DB): Command {
  return new DelCommand(db)
}
