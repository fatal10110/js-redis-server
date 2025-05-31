import { WrongNumberOfArguments, WrongType } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { StringDataType } from '../../../data-structures/string'
import { DB } from '../../../db'

export class GetCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments('get')
    }

    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments('get')
    }

    const val = this.db.get(args[0])

    if (val === null) return Promise.resolve({ response: null })

    if (!(val instanceof StringDataType)) {
      throw new WrongType(args[0].toString())
    }

    return Promise.resolve({ response: val.data })
  }
}

export default function (db: DB) {
  return new GetCommand(db)
}
