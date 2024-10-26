import { Command, CommandResult, Node } from '../../../../types'

export const commandName = 'setname'

export class ClientSetNameCommand implements Command {
  constructor(private readonly node: Node) {}

  getKeys(): Buffer[] {
    return []
  }

  run(): CommandResult {
    return {
      response: 'OK',
    }
  }
}
