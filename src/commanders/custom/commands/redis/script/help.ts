import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'

export class ScriptHelpCommand extends SchemaCommand<[]> {
  metadata = defineCommand('script|help', {
    arity: 1, // SCRIPT HELP
    flags: {
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], { transport }: CommandContext) {
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
    transport.write(helpText)
  }
}
