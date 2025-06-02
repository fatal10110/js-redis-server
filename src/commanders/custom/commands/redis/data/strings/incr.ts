import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'

export class IncrCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('incr')
    }
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('incr')
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof StringDataType)) {
      throw new WrongType()
    }

    let currentValue = 0
    if (existing instanceof StringDataType) {
      const valueStr = existing.data.toString()
      currentValue = parseInt(valueStr)
      if (isNaN(currentValue)) {
        throw new ExpectedInteger()
      }
    }

    const newValue = currentValue + 1
    this.db.set(key, new StringDataType(Buffer.from(newValue.toString())))

    return Promise.resolve({ response: newValue })
  }
}

export default function (db: DB) {
  return new IncrCommand(db)
}
