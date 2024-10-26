import { Command, CommandResult } from '../../../../types'

export class CommandsInfo implements Command {
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return []
  }
  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    return { response: 'mock response' } // TODO
  }
}

export default function () {
  return new CommandsInfo()
}
