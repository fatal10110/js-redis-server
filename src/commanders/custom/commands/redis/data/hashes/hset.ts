import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const HsetCommandDefinition: CommandDefinition = {
  metadata: defineCommand('hset', {
    arity: -4, // HSET key field value [field value ...]
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  }),
  factory: deps => new HsetCommand(deps.db),
}

export class HsetCommand implements Command {
  readonly metadata = HsetCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 3 || args.length % 2 === 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 3 || args.length % 2 === 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    let hash: HashDataType
    if (existing instanceof HashDataType) {
      hash = existing
    } else {
      hash = new HashDataType()
      this.db.set(key, hash)
    }

    let fieldsSet = 0
    for (let i = 1; i < args.length; i += 2) {
      const field = args[i]
      const value = args[i + 1]
      fieldsSet += hash.hset(field, value)
    }

    return Promise.resolve({ response: fieldsSet })
  }
}

export default function (db: DB) {
  return new HsetCommand(db)
}
