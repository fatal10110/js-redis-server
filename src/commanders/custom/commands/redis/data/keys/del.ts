import { WrongNumberOfArguments } from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'

export class DelCommand implements Command {
  constructor(private readonly db: DB) {}
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (!args.length) {
      throw new WrongNumberOfArguments('del')
    }

    let counter = 0

    for (const key of args) {
      if (this.db.del(key)) {
        counter++
      }
    }

    return Promise.resolve({ response: counter })
  }
}

export default function (db: DB): Command {
  return new DelCommand(db)
}
