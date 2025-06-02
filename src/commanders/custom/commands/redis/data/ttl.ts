import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { DB } from '../../../db'

export class TtlCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('ttl')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('ttl')
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: -2 }) // Key does not exist
    }

    const ttl = this.db.getTtl(key)
    if (ttl === -1) {
      return Promise.resolve({ response: -1 }) // Key exists but has no expiration
    }

    const remainingSeconds = Math.max(0, Math.ceil((ttl - Date.now()) / 1000))
    return Promise.resolve({ response: remainingSeconds })
  }
}

export default function (db: DB) {
  return new TtlCommand(db)
}
