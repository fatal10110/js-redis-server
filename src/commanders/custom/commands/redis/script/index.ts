import { UnknowScriptSubCommand } from '../../../../../core/errors'
import { Command } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext, t } from '../../../schema'
import { ScriptLoadCommand } from './load'
import { ScriptExistsCommand } from './exists'
import { ScriptFlushCommand } from './flush'
import { ScriptKillCommand } from './kill'
import { ScriptDebugCommand } from './debug'
import { ScriptHelpCommand } from './help'

const subCommands: Record<string, Command> = {
  load: new ScriptLoadCommand(),
  exists: new ScriptExistsCommand(),
  flush: new ScriptFlushCommand(),
  kill: new ScriptKillCommand(),
  debug: new ScriptDebugCommand(),
  help: new ScriptHelpCommand(),
}

export class ScriptCommand extends SchemaCommand<[Buffer, Buffer[]]> {
  metadata = defineCommand('script', {
    arity: -2, // SCRIPT subcommand [args...]
    flags: {
      admin: true,
      noscript: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  })

  protected schema = t.tuple([t.string(), t.variadic(t.string())])

  protected execute(
    [subCommandName, rest]: [Buffer, Buffer[]],
    ctx: CommandContext,
  ) {
    const subCommand = subCommands[subCommandName.toString().toLowerCase()]
    if (!subCommand) {
      throw new UnknowScriptSubCommand(subCommandName.toString())
    }
    subCommand.run(subCommandName, rest, ctx)
  }
}
