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
export const GetCommandDefinition: CommandDefinition = {
  metadata: defineCommand('get', {
    arity: 2, // GET key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0, // First arg is the key
    lastKey: 0, // Last arg is the key
    keyStep: 1, // Single key
    categories: [CommandCategory.STRING, CommandCategory.GENERIC],
  }),
  factory: deps => new GetCommand(deps.db),
}

export class GetCommand implements Command {
  readonly metadata = GetCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const val = this.db.get(args[0])

    if (val === null) return Promise.resolve({ response: null })

    if (!(val instanceof StringDataType)) {
      throw new WrongType()
    }

    return Promise.resolve({ response: val.data })
  }
}

export default function (db: DB) {
  return new GetCommand(db)
}
