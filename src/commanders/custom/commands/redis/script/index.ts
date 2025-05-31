import { Command, CommandResult } from '../../../../types'
import { UnknowScriptSubCommand, WrongNumberOfArguments } from '../../../errors'
import { ScriptLoadCommand } from './load'

export class ScriptCommand implements Command {
  constructor(private readonly subCommands: Record<string, Command>) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return []
  }
  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
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
  }

  return function () {
    return new ScriptCommand(subCommands)
  }
}
