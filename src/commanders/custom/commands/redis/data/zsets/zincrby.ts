import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedFloat,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const ZincrbyCommandDefinition: CommandDefinition = {
  metadata: defineCommand('zincrby', {
    arity: 4, // ZINCRBY key increment member
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0, // First arg is the key
    lastKey: 0, // Last arg is the key
    keyStep: 1, // Single key
    categories: [CommandCategory.ZSET],
  }),
  factory: deps => new ZincrbyCommand(deps.db),
}

export class ZincrbyCommand implements Command {
  readonly metadata = ZincrbyCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const incrementStr = args[1].toString()
    const member = args[2]

    const increment = parseFloat(incrementStr)
    if (isNaN(increment)) {
      throw new ExpectedFloat()
    }

    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    let zset: SortedSetDataType
    if (existing instanceof SortedSetDataType) {
      zset = existing
    } else {
      zset = new SortedSetDataType()
      this.db.set(key, zset)
    }

    const newScore = zset.zincrby(member, increment)
    return Promise.resolve({ response: Buffer.from(newScore.toString()) })
  }
}

export default function (db: DB) {
  return new ZincrbyCommand(db)
}
