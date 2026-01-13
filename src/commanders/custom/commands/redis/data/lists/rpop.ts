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
export const RpopCommandDefinition: CommandDefinition = {
  metadata: defineCommand('rpop', {
    arity: 2, // RPOP key
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  }),
  factory: deps => new RpopCommand(deps.db),
}

export class RpopCommand implements Command {
  readonly metadata = RpopCommandDefinition.metadata

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
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const value = existing.rpop()

    // Remove empty list from database
    if (existing.llen() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: value })
  }
}

export default function (db: DB) {
  return new RpopCommand(db)
}
