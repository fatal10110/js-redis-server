import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const ZrangeCommandDefinition: CommandDefinition = {
  metadata: defineCommand('zrange', {
    arity: -4, // ZRANGE key start stop [WITHSCORES]
    flags: {
      readonly: true,
    },
    firstKey: 0, // First arg is the key
    lastKey: 0, // Last arg is the key
    keyStep: 1, // Single key
    categories: [CommandCategory.ZSET],
  }),
  factory: deps => new ZrangeCommand(deps.db),
}

export class ZrangeCommand implements Command {
  readonly metadata = ZrangeCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 3) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 3) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const startStr = args[1].toString()
    const stopStr = args[2].toString()

    const start = parseInt(startStr)
    const stop = parseInt(stopStr)

    if (isNaN(start) || isNaN(stop)) {
      throw new ExpectedInteger()
    }

    // Check for WITHSCORES option
    let withScores = false
    if (args.length >= 4) {
      const option = args[3].toString().toUpperCase()
      if (option === 'WITHSCORES') {
        withScores = true
      } else {
        throw new WrongNumberOfArguments(this.metadata.name)
      }
    }

    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: [] })
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const result = existing.zrange(start, stop, withScores)
    return Promise.resolve({ response: result })
  }
}

export default function (db: DB) {
  return new ZrangeCommand(db)
}
