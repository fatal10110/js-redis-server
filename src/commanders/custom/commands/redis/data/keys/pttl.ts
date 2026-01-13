import { WrongNumberOfArguments } from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const PttlCommandDefinition: CommandDefinition = {
  metadata: defineCommand('pttl', {
    arity: 2, // PTTL key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.GENERIC],
  }),
  factory: deps => new PttlCommand(deps.db),
}

export class PttlCommand implements Command {
  readonly metadata = PttlCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
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

    const remainingMilliseconds = Math.max(0, ttl - Date.now())
    return Promise.resolve({ response: remainingMilliseconds })
  }
}

export default function (db: DB) {
  return new PttlCommand(db)
}
