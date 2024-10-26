import { Command, CommandBuilder, CommandResult, Node } from '../../../../types'
import {
  UnknwonClientSubCommand,
  WrongNumberOfArguments,
} from '../../../errors'
import {
  ClientSetNameCommand,
  commandName as setNameCommandName,
} from './clientSetName'

export const commandName = 'client'

export class ClientCommand implements Command {
  constructor(private readonly subCommands: Record<string, Command>) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): CommandResult {
    const subCommandName = args.shift()

    if (!subCommandName) {
      throw new WrongNumberOfArguments(commandName)
    }

    const subComamnd = this.subCommands[subCommandName.toString().toLowerCase()]

    if (!subComamnd) {
      throw new UnknwonClientSubCommand(subCommandName.toString())
    }

    return subComamnd.run(subCommandName, args)
  }
}

export default function (node: Node): CommandBuilder {
  const subCommands = {
    [setNameCommandName]: new ClientSetNameCommand(node),
  }

  return function (): Command {
    return new ClientCommand(subCommands)
  }
}
