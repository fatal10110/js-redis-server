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
export const HexistsCommandDefinition: CommandDefinition = {
  metadata: defineCommand('hexists', {
    arity: 3, // HEXISTS key field
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  }),
  factory: deps => new HexistsCommand(deps.db),
}

export class HexistsCommand implements Command {
  readonly metadata = HexistsCommandDefinition.metadata

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

    const key = args[0]
    const field = args[1]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: 0 })
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const exists = existing.hexists(field)
    return Promise.resolve({ response: exists ? 1 : 0 })
  }
}

export default function (db: DB) {
  return new HexistsCommand(db)
}
