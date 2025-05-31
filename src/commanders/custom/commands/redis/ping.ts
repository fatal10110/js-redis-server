import { Command, CommandResult } from '../../../../types'

export class Ping implements Command {
  getKeys(): Buffer[] {
    return []
  }
  run(): Promise<CommandResult> {
    return Promise.resolve({ response: 'PONG' })
  }
}

export default function () {
  return new Ping()
}
