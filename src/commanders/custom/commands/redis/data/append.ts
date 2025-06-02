import { WrongNumberOfArguments, WrongType } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { StringDataType } from '../../../data-structures/string'
import { DB } from '../../../db'

export class AppendCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('append')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('append')
    }

    const [key, value] = args
    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof StringDataType)) {
      throw new WrongType()
    }

    let newValue: Buffer
    if (existing instanceof StringDataType) {
      newValue = Buffer.concat([existing.data, value])
    } else {
      newValue = value
    }

    this.db.set(key, new StringDataType(newValue))

    return Promise.resolve({ response: newValue.length })
  }
}

export default function (db: DB) {
  return new AppendCommand(db)
}
