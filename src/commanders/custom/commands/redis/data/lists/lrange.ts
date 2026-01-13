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
export const LrangeCommandDefinition: CommandDefinition = {
  metadata: defineCommand('lrange', {
    arity: 4, // LRANGE key start stop
    flags: {
      readonly: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  }),
  factory: deps => new LrangeCommand(deps.db),
}

export class LrangeCommand implements Command {
  readonly metadata = LrangeCommandDefinition.metadata

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

    const [key, startArg, stopArg] = args
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: [] })
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const start = parseInt(startArg.toString())
    const stop = parseInt(stopArg.toString())

    if (isNaN(start) || isNaN(stop)) {
      throw new ExpectedInteger()
    }

    const result = existing.lrange(start, stop)
    return Promise.resolve({ response: result })
  }
}

export default function (db: DB) {
  return new LrangeCommand(db)
}
