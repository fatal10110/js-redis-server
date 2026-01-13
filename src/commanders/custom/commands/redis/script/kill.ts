import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const ScriptKillCommandDefinition: CommandDefinition = {
  metadata: defineCommand('script|kill', {
    arity: 1, // SCRIPT KILL
    flags: {
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  }),
  factory: () => new ScriptKillCommand(),
}

export class ScriptKillCommand implements Command {
  readonly metadata = ScriptKillCommandDefinition.metadata

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

    // In a real implementation, this would kill currently running scripts
    // For now, we'll just return OK since we don't have script execution tracking
    return Promise.resolve({ response: 'OK' })
  }
}
