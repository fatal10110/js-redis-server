import { WrongNumberOfArguments, WrongType } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { SetDataType } from '../../../data-structures/set'
import { DB } from '../../../db'

export class SismemberCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('sismember')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('sismember')
    }

    const key = args[0]
    const member = args[1]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: 0 })
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    const isMember = existing.sismember(member)
    return Promise.resolve({ response: isMember ? 1 : 0 })
  }
}

export default function (db: DB) {
  return new SismemberCommand(db)
}
