import crypto from 'crypto'
import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'

export class ScriptLoadCommand implements Command {
  constructor(private readonly scriptStore: Record<string, Buffer>) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (!args.length) {
      throw new WrongNumberOfArguments(`script|load`)
    }

    const shasum = crypto.createHash('sha1')
    shasum.update(args[0])
    const hash = shasum.digest('hex')

    if (!(hash in this.scriptStore)) {
      this.scriptStore[hash] = args[0]
    }

    return Promise.resolve({ response: hash })
  }
}
