import { Command, CommandResult } from '../../../../types'

export class QuitCommand implements Command {
  getKeys(): Buffer[] {
    return []
  }

  run(): Promise<CommandResult> {
    return Promise.resolve({
      close: true,
      response: 'OK',
    })
  }
}

export default function () {
  return new QuitCommand()
}
