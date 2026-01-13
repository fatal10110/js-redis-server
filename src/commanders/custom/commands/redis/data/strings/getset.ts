import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const GetsetCommandDefinition: CommandDefinition = {
  metadata: defineCommand('getset', {
    arity: 3, // GETSET key value
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  }),
  factory: deps => new GetsetCommand(deps.db),
}

export class GetsetCommand implements Command {
  readonly metadata = GetsetCommandDefinition.metadata

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
    const newValue = args[1]
    const existing = this.db.get(key)

    let oldValue: Buffer | null = null

    if (existing !== null) {
      if (!(existing instanceof StringDataType)) {
        throw new WrongType()
      }
      oldValue = existing.data
    }

    this.db.set(key, new StringDataType(newValue))
    return Promise.resolve({ response: oldValue })
  }
}

export default function (db: DB) {
  return new GetsetCommand(db)
}
