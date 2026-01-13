import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const ScriptLoadCommandDefinition: CommandDefinition = {
  metadata: defineCommand('script|load', {
    arity: 2, // SCRIPT LOAD <script>
    flags: {
      write: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  }),
  factory: deps => new ScriptLoadCommand(deps.db),
}

export class ScriptLoadCommand implements Command {
  readonly metadata = ScriptLoadCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const hash = this.db.addScript(args[0])

    return Promise.resolve({ response: hash })
  }
}
