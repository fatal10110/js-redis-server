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
export const HdelCommandDefinition: CommandDefinition = {
  metadata: defineCommand('hdel', {
    arity: -3, // HDEL key field [field ...]
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  }),
  factory: deps => new HdelCommand(deps.db),
}

export class HdelCommand implements Command {
  readonly metadata = HdelCommandDefinition.metadata

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
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: 0 })
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    let deletedCount = 0
    for (let i = 1; i < args.length; i++) {
      deletedCount += existing.hdel(args[i])
    }

    // Remove empty hash from database
    if (existing.hlen() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: deletedCount })
  }
}

export default function (db: DB) {
  return new HdelCommand(db)
}
