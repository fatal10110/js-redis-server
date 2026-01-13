import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const ScriptExistsCommandDefinition: CommandDefinition = {
  metadata: defineCommand('script|exists', {
    arity: -2, // SCRIPT EXISTS <sha1> [<sha1> ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  }),
  factory: deps => new ScriptExistsCommand(deps.db),
}

export class ScriptExistsCommand implements Command {
  readonly metadata = ScriptExistsCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (!args.length) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (!args.length) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const results: number[] = []

    for (const arg of args) {
      const hash = arg.toString()
      results.push(this.db.getScript(hash) ? 1 : 0)
    }

    return Promise.resolve({ response: results })
  }
}
