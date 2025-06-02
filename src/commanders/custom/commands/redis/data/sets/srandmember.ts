import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'

export class SrandmemberCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 1 || args.length > 2) {
      throw new WrongNumberOfArguments('srandmember')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 1 || args.length > 2) {
      throw new WrongNumberOfArguments('srandmember')
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing === null) {
      if (args.length === 2) {
        return Promise.resolve({ response: [] })
      }
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    if (args.length === 1) {
      // Single random member
      const member = existing.srandmember()
      return Promise.resolve({ response: member })
    }

    // Multiple random members with count
    const countStr = args[1].toString()
    const count = parseInt(countStr)
    if (isNaN(count)) {
      throw new ExpectedInteger()
    }

    const members = existing.srandmember(count)
    return Promise.resolve({ response: members })
  }
}

export default function (db: DB) {
  return new SrandmemberCommand(db)
}
