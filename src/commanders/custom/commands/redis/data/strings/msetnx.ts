import { WrongNumberOfArguments } from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'

export class MsetnxCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2 || args.length % 2 !== 0) {
      throw new WrongNumberOfArguments('msetnx')
    }

    const keys: Buffer[] = []
    for (let i = 0; i < args.length; i += 2) {
      keys.push(args[i])
    }
    return keys
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2 || args.length % 2 !== 0) {
      throw new WrongNumberOfArguments('msetnx')
    }

    // Check if any key already exists
    for (let i = 0; i < args.length; i += 2) {
      const key = args[i]
      if (this.db.get(key) !== null) {
        return Promise.resolve({ response: 0 })
      }
    }

    // If no keys exist, set all of them
    for (let i = 0; i < args.length; i += 2) {
      const key = args[i]
      const value = args[i + 1]
      this.db.set(key, new StringDataType(value))
    }

    return Promise.resolve({ response: 1 })
  }
}

export default function (db: DB) {
  return new MsetnxCommand(db)
}
