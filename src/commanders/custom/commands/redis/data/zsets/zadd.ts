import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedFloat,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'

export class ZaddCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 3) {
      throw new WrongNumberOfArguments('zadd')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 3) {
      throw new WrongNumberOfArguments('zadd')
    }

    if (args.length % 2 === 0) {
      throw new WrongNumberOfArguments('zadd')
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
