import { WrongNumberOfArguments } from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const MsetnxCommandDefinition: CommandDefinition = {
  metadata: defineCommand('msetnx', {
    arity: -3, // MSETNX key value [key value ...]
    flags: {
      write: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 2,
    categories: [CommandCategory.STRING],
  }),
  factory: deps => new MsetnxCommand(deps.db),
}

export class MsetnxCommand implements Command {
  readonly metadata = MsetnxCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2 || args.length % 2 !== 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const keys: Buffer[] = []
    for (let i = 0; i < args.length; i += 2) {
      keys.push(args[i])
    }
    return keys
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2 || args.length % 2 !== 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    // Check if any key already exists
    for (let i = 0; i < args.length; i += 2) {
      const key = args[i]
      if (this.db.get(key) !== null) {
        return Promise.resolve({ response: 0 })
      }
    }

    // If no keys exist, set all of them
    for (let i = 0; i < args.length; i += 2) {
      const key = args[i]
      const value = args[i + 1]
      this.db.set(key, new StringDataType(value))
    }

    return Promise.resolve({ response: 1 })
  }
}

export default function (db: DB) {
  return new MsetnxCommand(db)
}
