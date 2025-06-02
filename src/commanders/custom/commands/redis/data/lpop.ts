import { WrongNumberOfArguments, WrongType } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { ListDataType } from '../../../data-structures/list'
import { DB } from '../../../db'

export class LpopCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('lpop')
    }
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('lpop')
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const value = existing.lpop()

    // Remove empty list from database
    if (existing.llen() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: value })
  }
}

export default function (db: DB) {
  return new LpopCommand(db)
}
