import { DataCommand } from '..'
import { DB } from '../../../db'
import { WrongNumberOfArguments } from '../../../errors'

export class DelCommand implements DataCommand {
  getKeys(args: Buffer[]): Buffer[] {
    return args
  }

  run(db: DB, args: Buffer[]): number {
    if (!args.length) {
      throw new WrongNumberOfArguments('del')
    }

    let counter = 0

    for (const key of args) {
      if (db.del(key)) {
        counter++
      }
    }

    return counter
  }
}

export default new DelCommand()
