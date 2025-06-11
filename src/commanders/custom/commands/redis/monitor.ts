import { WrongNumberOfArguments } from '../../../../core/errors'
import { Command, CommandResult } from '../../../../types'

class MonitorCommand implements Command {
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length) {
      throw new WrongNumberOfArguments('monitor')
    }

    return []
  }
  async run(): Promise<CommandResult> {
    await new Promise(resolve => {
      setTimeout(resolve, 10000)
    })

    return {
      response: 'OK',
    }
  }
}

export default function () {
  return new MonitorCommand()
}
