import { DB } from './db'
import { UnknownCommand } from './errors'
import { Socket } from 'net'
import { Command, CommandProvider, CommandsInput } from '../types'

export class Commander implements CommandProvider {
  private readonly commandsMapper: WeakMap<Socket, Record<string, Command>> =
    new WeakMap()

  constructor(
    public readonly db: DB,
    private readonly commands: CommandsInput,
  ) {}

  getOrCreateCommand(socket: Socket, rawCmd: Buffer, args: Buffer[]): Command {
    let socketCommands = this.commandsMapper.get(socket)

    if (!socketCommands) {
      socketCommands = {}
      this.commandsMapper.set(socket, socketCommands)
    }

    let cmdName = rawCmd.toString().toLowerCase()

    if (cmdName in socketCommands) {
      return socketCommands[cmdName]
    }

    const builder = this.commands[cmdName]

    if (!builder) {
      throw new UnknownCommand(rawCmd, args)
    }

    return (socketCommands[cmdName] = builder(socket))
  }
}
