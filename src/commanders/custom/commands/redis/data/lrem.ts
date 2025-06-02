import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { ListDataType } from '../../../data-structures/list'
import { DB } from '../../../db'

export class LremCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('lrem')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('lrem')
    }

    const key = args[0]
    const countStr = args[1].toString()
    const value = args[2]

    const count = parseInt(countStr)
    if (isNaN(count)) {
      throw new ExpectedInteger()
    }

    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: 0 })
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const removed = existing.lrem(count, value)

    // Remove key if list is empty
    if (existing.llen() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: removed })
  }
}

export default function (db: DB) {
  return new LremCommand(db)
}
