import crypto from 'crypto'
import { Command, CommandResult } from '../../../../types'
import { WrongNumberOfArguments } from '../../../errors'

export class ScriptLoadCommand implements Command {
  constructor(private readonly scriptStore: Record<string, Buffer>) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    if (!args.length) {
      throw new WrongNumberOfArguments(`script|load`)
    }

    const shasum = crypto.createHash('sha1')
    shasum.update(args[0])
    const hash = shasum.digest('hex')

    if (!(hash in this.scriptStore)) {
      this.scriptStore[hash] = args[0]
    }

    return { response: hash }
  }
}
