import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'

export class ZrevrangeCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 3) {
      throw new WrongNumberOfArguments('zrevrange')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 3) {
      throw new WrongNumberOfArguments('zrevrange')
    }

    const key = args[0]
    const startStr = args[1].toString()
    const stopStr = args[2].toString()

    const start = parseInt(startStr)
    const stop = parseInt(stopStr)
    if (isNaN(start) || isNaN(stop)) {
      throw new ExpectedInteger()
    }

    let withScores = false
    if (args.length === 4) {
      const option = args[3].toString().toUpperCase()
      if (option === 'WITHSCORES') {
        withScores = true
      }
    }

    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: [] })
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const result = existing.zrevrange(start, stop, withScores)
    return Promise.resolve({ response: result })
  }
}

export default function (db: DB) {
  return new ZrevrangeCommand(db)
}
