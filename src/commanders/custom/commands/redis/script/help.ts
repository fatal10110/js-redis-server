import { Command, CommandResult } from '../../../../../types'

export class ScriptHelpCommand implements Command {
  getKeys(): Buffer[] {
    return []
  }

  run(_rawCmd: Buffer, _args: Buffer[]): Promise<CommandResult> {
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
