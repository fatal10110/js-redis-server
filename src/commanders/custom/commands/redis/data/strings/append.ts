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
export const AppendCommandDefinition: CommandDefinition = {
  metadata: defineCommand('append', {
    arity: 3, // APPEND key value
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
  factory: deps => new AppendCommand(deps.db),
}

export class AppendCommand implements Command {
  readonly metadata = AppendCommandDefinition.metadata

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

    const [key, value] = args
    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof StringDataType)) {
      throw new WrongType()
    }

    let newValue: Buffer
    if (existing instanceof StringDataType) {
      newValue = Buffer.concat([existing.data, value])
    } else {
      newValue = value
    }

    this.db.set(key, new StringDataType(newValue))

    return Promise.resolve({ response: newValue.length })
  }
}

export default function (db: DB) {
  return new AppendCommand(db)
}
