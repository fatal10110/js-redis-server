import { Command, CommandResult } from '../../../types'

export class InfoCommand implements Command {
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return []
  }
  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    return { response: 'mock info' }
  }
}

export default function () {
  return new InfoCommand()
}
