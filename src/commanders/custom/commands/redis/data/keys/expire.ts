import {
  WrongNumberOfArguments,
  ExpectedInteger,
  InvalidExpireTime,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'

export class ExpireCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('expire')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('expire')
    }

    const key = args[0]
    const secondsStr = args[1].toString()

    const seconds = parseInt(secondsStr)
    if (isNaN(seconds)) {
      throw new ExpectedInteger()
    }

    if (seconds < 0) {
      throw new InvalidExpireTime('expire')
    }

    // Special case: if seconds is 0, the key should be deleted immediately
    if (seconds === 0) {
      const deleted = this.db.del(key)
      return Promise.resolve({ response: deleted ? 1 : 0 })
    }

    const expiration = Date.now() + seconds * 1000
    const success = this.db.setExpiration(key, expiration)

    return Promise.resolve({ response: success ? 1 : 0 })
  }
}

export default function (db: DB) {
  return new ExpireCommand(db)
}
