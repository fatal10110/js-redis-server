import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { StringDataType } from '../../../data-structures/string'
import { DB } from '../../../db'

export class SetCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (!args.length) {
      throw new WrongNumberOfArguments('set')
    }

    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    const [key, val] = args

    if (!key || !val) {
      throw new WrongNumberOfArguments('set')
    }

    const existingData = this.db.get(key)

    if (!(existingData instanceof StringDataType)) {
      this.db.del(key)
    }

    // TODO handle flags
    this.db.set(key, new StringDataType(val))
    return Promise.resolve({ response: 'OK' })
  }
}

export default function (db: DB) {
  return new SetCommand(db)
}
