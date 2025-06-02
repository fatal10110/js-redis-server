import {
  UnknwonClientSubCommand,
  WrongNumberOfArguments,
} from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
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

  run(rawCommand: Buffer, args: Buffer[]): Promise<CommandResult> {
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

export default function () {
  const subCommands = {
    [setNameCommandName]: new ClientSetNameCommand(),
  }

  return new ClientCommand(subCommands)
}
