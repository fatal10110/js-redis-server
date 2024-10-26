import { Command, CommandResult } from '../../../types'

export class QuitCommand implements Command {
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    return {
      close: true,
      response: 'OK',
    }
  }
}

export default function () {
  return new QuitCommand()
}
