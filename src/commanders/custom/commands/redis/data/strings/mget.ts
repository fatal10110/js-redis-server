import { WrongNumberOfArguments } from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const MgetCommandDefinition: CommandDefinition = {
  metadata: defineCommand('mget', {
    arity: -2, // MGET key [key ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  }),
  factory: deps => new MgetCommand(deps.db),
}

export class MgetCommand implements Command {
  readonly metadata = MgetCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length === 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const res: (Buffer | null)[] = []

    for (let i = 0; i < args.length; i++) {
      const val = this.db.get(args[i])

      if (!(val instanceof StringDataType)) {
        res.push(null)
        continue
      }

      res.push(val.data)
    }

    return Promise.resolve({ response: res })
  }
}

export default function (db: DB) {
  return new MgetCommand(db)
}
