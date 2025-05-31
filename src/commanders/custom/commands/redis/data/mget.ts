import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { StringDataType } from '../../../data-structures/string'
import { DB } from '../../../db'

export class MgetCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length === 0) {
      throw new WrongNumberOfArguments('mget')
    }

    const res: (Buffer | null)[] = []

    for (const key of args) {
      const val = this.db.get(key)

      if (val instanceof StringDataType) {
        res.push(val.data)
      } else {
        res.push(null)
      }
    }

    return Promise.resolve({ response: res })
  }
}

export default function (db: DB) {
  return new MgetCommand(db)
}
