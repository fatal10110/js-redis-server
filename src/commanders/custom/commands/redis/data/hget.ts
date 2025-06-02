import { WrongNumberOfArguments, WrongType } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { HashDataType } from '../../../data-structures/hash'
import { DB } from '../../../db'

export class HgetCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('hget')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('hget')
    }

    const [key, field] = args
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const value = existing.hget(field)
    return Promise.resolve({ response: value })
  }
}

export default function (db: DB) {
  return new HgetCommand(db)
}
