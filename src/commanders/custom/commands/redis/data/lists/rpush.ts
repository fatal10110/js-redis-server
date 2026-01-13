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
export const RpushCommandDefinition: CommandDefinition = {
  metadata: defineCommand('rpush', {
    arity: -3, // RPUSH key element [element ...]
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  }),
  factory: deps => new RpushCommand(deps.db),
}

export class RpushCommand implements Command {
  readonly metadata = RpushCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    let list: ListDataType
    if (existing instanceof ListDataType) {
      list = existing
    } else {
      list = new ListDataType()
      this.db.set(key, list)
    }

    // Push all values in order
    for (let i = 1; i < args.length; i++) {
      list.rpush(args[i])
    }

    return Promise.resolve({ response: list.llen() })
  }
}

export default function (db: DB) {
  return new RpushCommand(db)
}
