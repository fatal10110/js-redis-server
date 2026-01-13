import { WrongNumberOfArguments } from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const DbSizeCommandDefinition: CommandDefinition = {
  metadata: defineCommand('dbsize', {
    arity: 1, // DBSIZE
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1, // No keys
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SERVER],
  }),
  factory: deps => new DbSizeCommand(deps.db),
}

export class DbSizeCommand implements Command {
  readonly metadata = DbSizeCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const size = this.db.size()

    return Promise.resolve({ response: size })
  }
}

export default function (db: DB) {
  return new DbSizeCommand(db)
}
