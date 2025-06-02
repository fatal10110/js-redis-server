import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedFloat,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { StringDataType } from '../../../data-structures/string'
import { DB } from '../../../db'

export class IncrbyfloatCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('incrbyfloat')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('incrbyfloat')
    }

    const key = args[0]
    const incrementStr = args[1].toString()

    const increment = parseFloat(incrementStr)
    if (isNaN(increment)) {
      throw new ExpectedFloat()
    }

    const existing = this.db.get(key)
    let currentValue = 0

    if (existing !== null) {
      if (!(existing instanceof StringDataType)) {
        throw new WrongType()
      }

      currentValue = parseFloat(existing.data.toString())
      if (isNaN(currentValue)) {
        throw new ExpectedFloat()
      }
    }

    const newValue = currentValue + increment
    this.db.set(key, new StringDataType(Buffer.from(newValue.toString())))

    return Promise.resolve({ response: Buffer.from(newValue.toString()) })
  }
}

export default function (db: DB) {
  return new IncrbyfloatCommand(db)
}
