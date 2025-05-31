import { Command, CommandResult } from '../../../../types'

export class InfoCommand implements Command {
  getKeys(): Buffer[] {
    return []
  }
  run(): Promise<CommandResult> {
    return Promise.resolve({ response: 'mock info' })
  }
}

export default function () {
  return new InfoCommand()
}
