import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'

export class LrangeCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('lrange')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('lrange')
    }

    const [key, startArg, stopArg] = args
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: [] })
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const start = parseInt(startArg.toString())
    const stop = parseInt(stopArg.toString())

    if (isNaN(start) || isNaN(stop)) {
      throw new ExpectedInteger()
    }

    const result = existing.lrange(start, stop)
    return Promise.resolve({ response: result })
  }
}

export default function (db: DB) {
  return new LrangeCommand(db)
}
