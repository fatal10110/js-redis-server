import { Command, CommandResult } from '../../../../../types'

export const commandName = 'setname'

export class ClientSetNameCommand implements Command {
  constructor() {}

  getKeys(): Buffer[] {
    return []
  }

  run(): Promise<CommandResult> {
    return Promise.resolve({
      response: 'OK',
    })
  }
}
