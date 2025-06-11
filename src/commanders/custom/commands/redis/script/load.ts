import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { DB } from '../../../db'

export class ScriptLoadCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (!args.length) {
      throw new WrongNumberOfArguments(`script|load`)
    }

    const hash = this.db.addScript(args[0])

    return Promise.resolve({ response: hash })
  }
}
