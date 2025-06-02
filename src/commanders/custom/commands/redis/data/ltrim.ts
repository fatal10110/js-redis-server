import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { ListDataType } from '../../../data-structures/list'
import { DB } from '../../../db'

export class LtrimCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('ltrim')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('ltrim')
    }

    const key = args[0]
    const startStr = args[1].toString()
    const stopStr = args[2].toString()

    const start = parseInt(startStr)
    const stop = parseInt(stopStr)
    if (isNaN(start) || isNaN(stop)) {
      throw new ExpectedInteger()
    }

    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: 'OK' })
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    existing.ltrim(start, stop)

    // Remove key if list is empty
    if (existing.llen() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: 'OK' })
  }
}

export default function (db: DB) {
  return new LtrimCommand(db)
}
