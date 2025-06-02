import { WrongNumberOfArguments, WrongType } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { SetDataType } from '../../../data-structures/set'
import { DB } from '../../../db'

export class SremCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2) {
      throw new WrongNumberOfArguments('srem')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2) {
      throw new WrongNumberOfArguments('srem')
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: 0 })
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    let removedCount = 0
    for (let i = 1; i < args.length; i++) {
      removedCount += existing.srem(args[i])
    }

    // Remove empty set from database
    if (existing.scard() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: removedCount })
  }
}

export default function (db: DB) {
  return new SremCommand(db)
}
