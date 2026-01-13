import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const ScriptHelpCommandDefinition: CommandDefinition = {
  metadata: defineCommand('script|help', {
    arity: 1, // SCRIPT HELP
    flags: {
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  }),
  factory: () => new ScriptHelpCommand(),
}

export class ScriptHelpCommand implements Command {
  readonly metadata = ScriptHelpCommandDefinition.metadata

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

    const helpText = [
      'SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
      'DEBUG <YES|SYNC|NO>',
      '    Set the debug mode for subsequent scripts executed.',
      'EXISTS <sha1> [<sha1> ...]',
      '    Check if scripts exist in the script cache by SHA1 digest.',
      'FLUSH [ASYNC|SYNC]',
      '    Flush the Lua scripts cache. Very dangerous on replicas.',
      'HELP',
      '    Prints this help.',
      'KILL',
      '    Kill the currently executing Lua script.',
      'LOAD <script>',
      '    Load a script into the scripts cache without executing it.',
    ]

    return Promise.resolve({ response: helpText })
  }
}
