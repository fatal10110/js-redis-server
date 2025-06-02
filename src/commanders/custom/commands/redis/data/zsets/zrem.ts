import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'

export class ZremCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2) {
      throw new WrongNumberOfArguments('zrem')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2) {
      throw new WrongNumberOfArguments('zrem')
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
