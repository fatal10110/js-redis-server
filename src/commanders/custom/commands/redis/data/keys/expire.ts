import {
  WrongNumberOfArguments,
  ExpectedInteger,
  InvalidExpireTime,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const ExpireCommandDefinition: CommandDefinition = {
  metadata: defineCommand('expire', {
    arity: 3, // EXPIRE key seconds
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.GENERIC],
  }),
  factory: deps => new ExpireCommand(deps.db),
}

export class ExpireCommand implements Command {
  readonly metadata = ExpireCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const secondsStr = args[1].toString()

    const seconds = parseInt(secondsStr)
    if (isNaN(seconds)) {
      throw new ExpectedInteger()
    }

    if (seconds < 0) {
      throw new InvalidExpireTime(this.metadata.name)
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
