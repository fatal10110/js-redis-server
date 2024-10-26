import { Command, CommandResult } from '../../../types'

export class Ping implements Command {
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return []
  }
  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    return { response: 'PONG' }
  }
}

export default function () {
  return new Ping()
}
