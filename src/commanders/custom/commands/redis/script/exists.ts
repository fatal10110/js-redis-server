import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'

export class ScriptExistsCommand implements Command {
  constructor(private readonly scriptStore: Record<string, Buffer>) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (!args.length) {
      throw new WrongNumberOfArguments('script|exists')
    }

    const results: number[] = []

    for (const arg of args) {
      const hash = arg.toString()
      results.push(hash in this.scriptStore ? 1 : 0)
    }

    return Promise.resolve({ response: results })
  }
}
