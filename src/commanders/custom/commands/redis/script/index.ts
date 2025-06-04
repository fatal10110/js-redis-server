import {
  UnknowScriptSubCommand,
  WrongNumberOfArguments,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { ScriptLoadCommand } from './load'
import { ScriptExistsCommand } from './exists'
import { ScriptFlushCommand } from './flush'
import { ScriptKillCommand } from './kill'
import { ScriptDebugCommand } from './debug'
import { ScriptHelpCommand } from './help'

export class ScriptCommand implements Command {
  constructor(private readonly subCommands: Record<string, Command>) {}

  getKeys(): Buffer[] {
    return []
  }
  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    const subCommandName = args.pop()

    if (!subCommandName) {
      throw new WrongNumberOfArguments(rawCmd.toString())
    }

    const subComamnd = this.subCommands[subCommandName.toString().toLowerCase()]

    if (!subComamnd) {
      throw new UnknowScriptSubCommand(subCommandName.toString())
    }

    return subComamnd.run(subCommandName, args)
  }
}

export default function (scriptsStore: Record<string, Buffer>) {
  const subCommands = {
    load: new ScriptLoadCommand(scriptsStore),
    exists: new ScriptExistsCommand(scriptsStore),
    flush: new ScriptFlushCommand(scriptsStore),
    kill: new ScriptKillCommand(),
    debug: new ScriptDebugCommand(),
    help: new ScriptHelpCommand(),
  }

  return new ScriptCommand(subCommands)
}
