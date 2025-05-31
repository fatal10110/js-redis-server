import { Command, CommandResult } from '../../../../../types'

export class CommandsInfo implements Command {
  getKeys(): Buffer[] {
    return []
  }
  run(): Promise<CommandResult> {
    return Promise.resolve({ response: 'mock response' }) // TODO
  }
}

export default function () {
  return new CommandsInfo()
}
