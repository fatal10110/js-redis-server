import { DataCommand } from '.'
import { StringDataType } from '../../data-structures/string'
import { DB } from '../db'
import { WrongNumberOfArguments, WrongType } from '../errors'

export class GetCommand implements DataCommand {
  getKeys(args: Buffer[]): Buffer[] {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments('get')
    }

    return args
  }

  run(db: DB, args: Buffer[]): unknown {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments('get')
    }

    const val = db.get(args[0])

    if (!val) return null

    if (!(val instanceof StringDataType)) {
      throw new WrongType(args[0].toString())
    }

    return val.data
  }
}

export default new GetCommand()
