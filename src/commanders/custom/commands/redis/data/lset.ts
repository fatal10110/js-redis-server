import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { ListDataType } from '../../../data-structures/list'
import { DB } from '../../../db'

// Create custom error for index out of range
class IndexOutOfRange extends Error {
  constructor() {
    super('index out of range')
    this.name = 'ERR'
  }
}

export class LsetCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('lset')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments('lset')
    }

    const key = args[0]
    const indexStr = args[1].toString()
    const value = args[2]

    const index = parseInt(indexStr)
    if (isNaN(index)) {
      throw new ExpectedInteger()
    }

    const existing = this.db.get(key)

    if (existing === null) {
      throw new IndexOutOfRange()
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const success = existing.lset(index, value)
    if (!success) {
      throw new IndexOutOfRange()
    }

    return Promise.resolve({ response: 'OK' })
  }
}

export default function (db: DB) {
  return new LsetCommand(db)
}
