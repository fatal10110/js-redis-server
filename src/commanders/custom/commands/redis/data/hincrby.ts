import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { HashDataType } from '../../../data-structures/hash'
import { DB } from '../../../db'

export class HincrbyCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('hincrby')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('hincrby')
    }

    const key = args[0]
    const field = args[1]
    const incrementStr = args[2].toString()

    const increment = parseInt(incrementStr)
    if (isNaN(increment)) {
      throw new ExpectedInteger()
    }

    const existing = this.db.get(key)
    let hash: HashDataType

    if (existing === null) {
      hash = new HashDataType()
      this.db.set(key, hash)
    } else {
      if (!(existing instanceof HashDataType)) {
        throw new WrongType()
      }
      hash = existing
    }

    const newValue = hash.hincrby(field, increment)
    return Promise.resolve({ response: newValue })
  }
}

export default function (db: DB) {
  return new HincrbyCommand(db)
}
