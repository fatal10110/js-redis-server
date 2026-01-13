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
export const DecrbyCommandDefinition: CommandDefinition = {
  metadata: defineCommand('decrby', {
    arity: 3, // DECRBY key decrement
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  }),
  factory: deps => new DecrbyCommand(deps.db),
}

export class DecrbyCommand implements Command {
  readonly metadata = DecrbyCommandDefinition.metadata

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
    const decrementStr = args[1].toString()

    const decrement = parseInt(decrementStr)
    if (isNaN(decrement)) {
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

    const newValue = currentValue - decrement
    this.db.set(key, new StringDataType(Buffer.from(newValue.toString())))

    return Promise.resolve({ response: newValue })
  }
}

export default function (db: DB) {
  return new DecrbyCommand(db)
}
