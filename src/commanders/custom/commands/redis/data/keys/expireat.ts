import {
  WrongNumberOfArguments,
  ExpectedInteger,
  InvalidExpireTime,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'

export class ExpireatCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('expireat')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('expireat')
    }

    const key = args[0]
    const timestampStr = args[1].toString()

    const timestamp = parseInt(timestampStr)
    if (isNaN(timestamp)) {
      throw new ExpectedInteger()
    }

    if (timestamp < 0) {
      throw new InvalidExpireTime('expireat')
    }

    const expiration = timestamp * 1000 // Convert seconds to milliseconds
    const now = Date.now()

    // If the timestamp is in the past, delete the key immediately
    if (expiration <= now) {
      const deleted = this.db.del(key)
      return Promise.resolve({ response: deleted ? 1 : 0 })
    }

    const success = this.db.setExpiration(key, expiration)

    return Promise.resolve({ response: success ? 1 : 0 })
  }
}

export default function (db: DB) {
  return new ExpireatCommand(db)
}
