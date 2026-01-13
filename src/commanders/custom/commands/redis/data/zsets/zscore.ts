import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const ZscoreCommandDefinition: CommandDefinition = {
  metadata: defineCommand('zscore', {
    arity: 3, // ZSCORE key member
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0, // First arg is the key
    lastKey: 0, // Last arg is the key
    keyStep: 1, // Single key
    categories: [CommandCategory.ZSET],
  }),
  factory: deps => new ZscoreCommand(deps.db),
}

export class ZscoreCommand implements Command {
  readonly metadata = ZscoreCommandDefinition.metadata

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
    const member = args[1]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const score = existing.zscore(member)
    const response = score !== null ? Buffer.from(score.toString()) : null
    return Promise.resolve({ response })
  }
}

export default function (db: DB) {
  return new ZscoreCommand(db)
}
