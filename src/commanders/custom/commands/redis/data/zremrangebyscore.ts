import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedFloat,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { SortedSetDataType } from '../../../data-structures/zset'
import { DB } from '../../../db'

export class ZremrangebyscoreCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('zremrangebyscore')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('zremrangebyscore')
    }

    const key = args[0]
    const minStr = args[1].toString()
    const maxStr = args[2].toString()

    const min = parseFloat(minStr)
    const max = parseFloat(maxStr)

    if (isNaN(min) || isNaN(max)) {
      throw new ExpectedFloat()
    }

    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: 0 })
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const removedCount = existing.zremrangebyscore(min, max)

    // Remove the key if the sorted set is empty
    if (existing.zcard() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: removedCount })
  }
}

export default function createZremrangebyscore(db: DB): Command {
  return new ZremrangebyscoreCommand(db)
}
