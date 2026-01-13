import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedFloat,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const ZaddCommandDefinition: CommandDefinition = {
  metadata: defineCommand('zadd', {
    arity: -4, // ZADD key score member [score member ...]
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0, // First arg is the key
    lastKey: 0, // Last arg is the key
    keyStep: 1, // Single key
    categories: [CommandCategory.ZSET],
  }),
  factory: deps => new ZaddCommand(deps.db),
}

export class ZaddCommand implements Command {
  readonly metadata = ZaddCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 3) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 3) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    if (args.length % 2 === 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const scoreMemberPairs: Array<{ score: number; member: Buffer }> = []

    // Parse score-member pairs
    for (let i = 1; i < args.length; i += 2) {
      const scoreStr = args[i].toString()
      const member = args[i + 1]

      const score = parseFloat(scoreStr)
      if (isNaN(score)) {
        throw new ExpectedFloat()
      }

      scoreMemberPairs.push({ score, member })
    }

    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    let zset: SortedSetDataType
    if (existing instanceof SortedSetDataType) {
      zset = existing
    } else {
      zset = new SortedSetDataType()
      this.db.set(key, zset)
    }

    let addedCount = 0
    for (const { score, member } of scoreMemberPairs) {
      addedCount += zset.zadd(score, member)
    }

    return Promise.resolve({ response: addedCount })
  }
}

export default function (db: DB) {
  return new ZaddCommand(db)
}
