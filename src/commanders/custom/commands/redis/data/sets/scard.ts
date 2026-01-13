import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const ScardCommandDefinition: CommandDefinition = {
  metadata: defineCommand('scard', {
    arity: 2, // SCARD key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  }),
  factory: deps => new ScardCommand(deps.db),
}

export class ScardCommand implements Command {
  readonly metadata = ScardCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: 0 })
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    return Promise.resolve({ response: existing.scard() })
  }
}

export default function (db: DB) {
  return new ScardCommand(db)
}
