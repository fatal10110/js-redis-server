import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { DB } from '../../../db'

export class ExistsCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length === 0) {
      throw new WrongNumberOfArguments('exists')
    }
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length === 0) {
      throw new WrongNumberOfArguments('exists')
    }

    let count = 0
    for (let i = 0; i < args.length; i++) {
      if (this.db.get(args[i]) !== null) {
        count++
      }
    }

    return Promise.resolve({ response: count })
  }
}

export default function (db: DB) {
  return new ExistsCommand(db)
}
