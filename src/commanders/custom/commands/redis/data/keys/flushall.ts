import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'
import { WrongNumberOfArguments } from '../../../../../../core/errors'

// Command definition with metadata
export const FlushallCommandDefinition: CommandDefinition = {
  metadata: defineCommand('flushall', {
    arity: 1, // FLUSHALL
    flags: {
      write: true,
    },
    firstKey: -1, // No keys
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.GENERIC, CommandCategory.SERVER],
  }),
  factory: deps => new FlushallCommand(deps.db),
}

export class FlushallCommand implements Command {
  readonly metadata = FlushallCommandDefinition.metadata

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

    this.db.flushall()
    return Promise.resolve({ response: 'OK' })
  }
}

export default function (db: DB): Command {
  return new FlushallCommand(db)
}
