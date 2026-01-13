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

// Create custom error for index out of range
class IndexOutOfRange extends Error {
  constructor() {
    super('index out of range')
    this.name = 'ERR'
  }
}

// Command definition with metadata
export const LsetCommandDefinition: CommandDefinition = {
  metadata: defineCommand('lset', {
    arity: 4, // LSET key index element
    flags: {
      write: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  }),
  factory: deps => new LsetCommand(deps.db),
}

export class LsetCommand implements Command {
  readonly metadata = LsetCommandDefinition.metadata

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
    const indexStr = args[1].toString()
    const value = args[2]

    const index = parseInt(indexStr)
    if (isNaN(index)) {
      throw new ExpectedInteger()
    }

    const existing = this.db.get(key)

    if (existing === null) {
      throw new IndexOutOfRange()
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const success = existing.lset(index, value)
    if (!success) {
      throw new IndexOutOfRange()
    }

    return Promise.resolve({ response: 'OK' })
  }
}

export default function (db: DB) {
  return new LsetCommand(db)
}
