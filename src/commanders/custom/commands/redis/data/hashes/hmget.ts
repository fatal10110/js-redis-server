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
export const HmgetCommandDefinition: CommandDefinition = {
  metadata: defineCommand('hmget', {
    arity: -3, // HMGET key field [field ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  }),
  factory: deps => new HmgetCommand(deps.db),
}

export class HmgetCommand implements Command {
  readonly metadata = HmgetCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const fields = args.slice(1)
    const existing = this.db.get(key)

    if (existing === null) {
      // Return array of nulls for all requested fields
      return Promise.resolve({ response: fields.map(() => null) })
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const result = existing.hmget(fields)
    return Promise.resolve({ response: result })
  }
}

export default function (db: DB) {
  return new HmgetCommand(db)
}
