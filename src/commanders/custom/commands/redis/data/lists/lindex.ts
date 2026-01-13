import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const LindexCommandDefinition: CommandDefinition = {
  metadata: defineCommand('lindex', {
    arity: 3, // LINDEX key index
    flags: {
      readonly: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  }),
  factory: deps => new LindexCommand(deps.db),
}

export class LindexCommand implements Command {
  readonly metadata = LindexCommandDefinition.metadata

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
    const indexStr = args[1].toString()

    const index = parseInt(indexStr)
    if (isNaN(index)) {
      throw new ExpectedInteger()
    }

    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const result = existing.lindex(index)
    return Promise.resolve({ response: result })
  }
}

export default function (db: DB) {
  return new LindexCommand(db)
}
