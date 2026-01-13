import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'
import { WrongNumberOfArguments } from '../../../../../../core/errors'

// Command definition with metadata
export const FlushdbCommandDefinition: CommandDefinition = {
  metadata: defineCommand('flushdb', {
    arity: 1, // FLUSHDB
    flags: {
      write: true,
    },
    firstKey: -1, // No keys
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.GENERIC, CommandCategory.SERVER],
  }),
  factory: deps => new FlushdbCommand(deps.db),
}

export class FlushdbCommand implements Command {
  readonly metadata = FlushdbCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(_rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    this.db.flushdb()
    return Promise.resolve({ response: 'OK' })
  }
}

export default function (db: DB): Command {
  return new FlushdbCommand(db)
}
