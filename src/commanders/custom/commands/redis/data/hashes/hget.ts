import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const HgetCommandDefinition: CommandDefinition = {
  metadata: defineCommand('hget', {
    arity: 3, // HGET key field
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  }),
  factory: deps => new HgetCommand(deps.db),
}

export class HgetCommand implements Command {
  readonly metadata = HgetCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const [key, field] = args
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const value = existing.hget(field)
    return Promise.resolve({ response: value })
  }
}

export default function (db: DB) {
  return new HgetCommand(db)
}
