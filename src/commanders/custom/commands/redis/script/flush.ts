import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const ScriptFlushCommandDefinition: CommandDefinition = {
  metadata: defineCommand('script|flush', {
    arity: -1, // SCRIPT FLUSH [ASYNC|SYNC]
    flags: {
      write: true,
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  }),
  factory: deps => new ScriptFlushCommand(deps.db),
}

export class ScriptFlushCommand implements Command {
  readonly metadata = ScriptFlushCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length > 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(_rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length > 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    this.db.flushScripts()

    return Promise.resolve({ response: 'OK' })
  }
}
