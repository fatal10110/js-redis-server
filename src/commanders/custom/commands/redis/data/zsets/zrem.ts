import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const ZremCommandDefinition: CommandDefinition = {
  metadata: defineCommand('zrem', {
    arity: -3, // ZREM key member [member ...]
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0, // First arg is the key
    lastKey: 0, // Last arg is the key
    keyStep: 1, // Single key
    categories: [CommandCategory.ZSET],
  }),
  factory: deps => new ZremCommand(deps.db),
}

export class ZremCommand implements Command {
  readonly metadata = ZremCommandDefinition.metadata

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

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    let removedCount = 0
    for (let i = 1; i < args.length; i++) {
      removedCount += existing.zrem(args[i])
    }

    // Remove the key if the sorted set is empty
    if (existing.zcard() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: removedCount })
  }
}

export default function (db: DB) {
  return new ZremCommand(db)
}
