import { WrongNumberOfArguments } from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const MsetCommandDefinition: CommandDefinition = {
  metadata: defineCommand('mset', {
    arity: -3, // MSET key value [key value ...]
    flags: {
      write: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 2,
    categories: [CommandCategory.STRING],
  }),
  factory: deps => new MsetCommand(deps.db),
}

export class MsetCommand implements Command {
  readonly metadata = MsetCommandDefinition.metadata

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

    for (let i = 0; i < args.length; i += 2) {
      const key = args[i]
      const value = args[i + 1]
      this.db.set(key, new StringDataType(value))
    }

    return Promise.resolve({ response: 'OK' })
  }
}

export default function (db: DB) {
  return new MsetCommand(db)
}
