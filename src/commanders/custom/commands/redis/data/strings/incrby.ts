import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const IncrbyCommandDefinition: CommandDefinition = {
  metadata: defineCommand('incrby', {
    arity: 3, // INCRBY key increment
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  }),
  factory: deps => new IncrbyCommand(deps.db),
}

export class IncrbyCommand implements Command {
  readonly metadata = IncrbyCommandDefinition.metadata

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
    const incrementStr = args[1].toString()

    const increment = parseInt(incrementStr)
    if (isNaN(increment)) {
      throw new ExpectedInteger()
    }

    const existing = this.db.get(key)
    let currentValue = 0

    if (existing !== null) {
      if (!(existing instanceof StringDataType)) {
        throw new WrongType()
      }

      currentValue = parseInt(existing.data.toString())
      if (isNaN(currentValue)) {
        throw new ExpectedInteger()
      }
    }

    const newValue = currentValue + increment
    this.db.set(key, new StringDataType(Buffer.from(newValue.toString())))

    return Promise.resolve({ response: newValue })
  }
}

export default function (db: DB) {
  return new IncrbyCommand(db)
}
