import {
  UnknowScriptSubCommand,
  WrongNumberOfArguments,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'
import { ScriptLoadCommand } from './load'
import { ScriptExistsCommand } from './exists'
import { ScriptFlushCommand } from './flush'
import { ScriptKillCommand } from './kill'
import { ScriptDebugCommand } from './debug'
import { ScriptHelpCommand } from './help'
import { DB } from '../../../db'

export const ScriptCommandDefinition: CommandDefinition = {
  metadata: defineCommand('script', {
    arity: -2, // SCRIPT subcommand [args...]
    flags: {
      admin: true,
      noscript: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  }),
  factory: deps => new ScriptCommand(createSubCommands(deps.db)),
}

export class ScriptCommand implements Command {
  readonly metadata = ScriptCommandDefinition.metadata

  constructor(private readonly subCommands: Record<string, Command>) {}

  getKeys(_rawCmd: Buffer, _args: Buffer[]): Buffer[] {
    return []
  }
  run(
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<CommandResult> {
    const subCommandName = args.shift()

    if (!subCommandName) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const subComamnd = this.subCommands[subCommandName.toString().toLowerCase()]

    if (!subComamnd) {
      throw new UnknowScriptSubCommand(subCommandName.toString())
    }

    return subComamnd.run(subCommandName, args, signal)
  }
}

export default function (db: DB) {
  return new ScriptCommand(createSubCommands(db))
}

function createSubCommands(db: DB): Record<string, Command> {
  return {
    load: new ScriptLoadCommand(db),
    exists: new ScriptExistsCommand(db),
    flush: new ScriptFlushCommand(db),
    kill: new ScriptKillCommand(),
    debug: new ScriptDebugCommand(),
    help: new ScriptHelpCommand(),
  }
}
