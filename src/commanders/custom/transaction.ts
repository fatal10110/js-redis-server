import {
  TransactionDiscardedWithError,
  UnknownCommand,
  UserFacedError,
  WrongNumberOfArguments,
} from '../../core/errors'
import { Command, CommandResult } from '../../types'

type BufferedCommand = {
  cmd: Command
  args: Buffer[]
  rawCmd: Buffer
}

export class TransactionCommand implements Command {
  private readonly bufferedCommands: (BufferedCommand | UserFacedError)[] = []
  private shouldDiscard = false

  constructor(private readonly commands: Record<string, Command>) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    const cmdName = rawCmd.toString().toLowerCase()

    if (cmdName === 'exec') {
      return []
    }

    try {
      const cmdName = rawCmd.toString().toLowerCase()
      const cmd = this.commands[cmdName]

      if (!cmd) {
        const err = new UnknownCommand(rawCmd, args)
        this.bufferedCommands.push(err)
        throw err
      }

      return cmd.getKeys(rawCmd, args)
    } catch (err) {
      if (
        err instanceof WrongNumberOfArguments ||
        err instanceof UnknownCommand
      ) {
        this.shouldDiscard = true
      }

      throw err
    }
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    const cmdName = rawCmd.toString().toLowerCase()

    if (cmdName === 'exec') {
      if (this.shouldDiscard) {
        throw new TransactionDiscardedWithError()
      }

      return this.execBuffer()
    }

    const cmd = this.commands[cmdName]

    if (!cmd) {
      const err = new UnknownCommand(rawCmd, args)
      this.bufferedCommands.push(err)
      this.shouldDiscard = true
      throw err
    }

    this.bufferedCommands.push({
      rawCmd,
      args,
      cmd,
    })

    return Promise.resolve({ response: 'QUEUED' })
  }

  shutdown(): Promise<void> {
    throw new Error('Method not implemented.')
  }

  async execBuffer(): Promise<CommandResult> {
    const results = []
    try {
      for (const buff of this.bufferedCommands) {
        if (buff instanceof UserFacedError) {
          throw buff
        }

        const result = await buff.cmd.run(buff.rawCmd, buff.args)

        if (result.close) {
          return result
        }

        results.push(result.response)
      }
    } catch (err) {
      // TODO do not repeat this logic
      if (err instanceof UserFacedError) {
        results.push(err)
      }

      throw err
    }

    return { response: results }
  }
}
