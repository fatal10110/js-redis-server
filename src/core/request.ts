import { Command } from './commands'
import { DB } from './db'
import { UnknownCommand } from './errors'

export class RequestHandler {
  constructor(
    private readonly db: DB,
    private readonly commands: Record<string, Command>,
  ) {}

  handleRequest(rawCmd: Buffer, args: Buffer[]) {
    const cmd = rawCmd.toString().toLowerCase()

    switch (cmd) {
      case 'command':
        return 'mock command'
      case 'info':
        return 'mock info'
      case 'ping':
        return 'PONG'
    }

    if (!(cmd in this.commands)) {
      throw new UnknownCommand(rawCmd, args)
    }

    return this.commands[cmd](this.db, args)
  }
}
