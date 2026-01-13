import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const LlenCommandDefinition: CommandDefinition = {
  metadata: defineCommand('llen', {
    arity: 2, // LLEN key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  }),
  factory: deps => new LlenCommand(deps.db),
}

export class LlenCommand implements Command {
  readonly metadata = LlenCommandDefinition.metadata

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

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    return Promise.resolve({ response: existing.llen() })
  }
}

export default function (db: DB) {
  return new LlenCommand(db)
}
