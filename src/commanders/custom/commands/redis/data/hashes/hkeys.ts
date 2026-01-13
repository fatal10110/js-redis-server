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
export const HkeysCommandDefinition: CommandDefinition = {
  metadata: defineCommand('hkeys', {
    arity: 2, // HKEYS key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  }),
  factory: deps => new HkeysCommand(deps.db),
}

export class HkeysCommand implements Command {
  readonly metadata = HkeysCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: [] })
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const result = existing.hkeys()
    return Promise.resolve({ response: result })
  }
}

export default function (db: DB) {
  return new HkeysCommand(db)
}
