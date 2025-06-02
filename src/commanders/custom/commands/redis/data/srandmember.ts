import { WrongNumberOfArguments, WrongType } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { SetDataType } from '../../../data-structures/set'
import { DB } from '../../../db'

export class SrandmemberCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('srandmember')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('srandmember')
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    const member = existing.srandmember()
    return Promise.resolve({ response: member })
  }
}

export default function (db: DB) {
  return new SrandmemberCommand(db)
}
