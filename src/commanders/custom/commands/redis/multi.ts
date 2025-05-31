import {
  TransactionDiscardedWithError,
  TransactionDiscardedWithReson,
  UnknownCommand,
  UserFacedError,
  WrongNumberOfArguments,
} from '../../../../core/errors'
import { Command, DBCommandExecutor, CommandResult } from '../../../../types'

type BufferedCommand = {
  cmd: Command
  args: Buffer[]
  rawCmd: Buffer
}

export interface TransactionalCommander extends DBCommandExecutor {
  setTransaction(transaction: DBCommandExecutor | null): void
}

class MultiCommander implements DBCommandExecutor {
  private readonly bufferedCommands: (BufferedCommand | UserFacedError)[] = []
  private readonly multiCommands: Record<string, Command>

  constructor(
    private readonly transactionalCommander: TransactionalCommander,
    private readonly commands: Record<string, Command>,
  ) {
    this.multiCommands = {
      exec: new ExecCommand(this.transactionalCommander, this),
      discard: new DiscardCommand(this.transactionalCommander),
    }
  }

  execute(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    const cmdName = rawCmd.toString().toLowerCase()
    let cmd = this.multiCommands[cmdName]

    if (cmd) {
      return cmd.run(rawCmd, args)
    }

    cmd = this.commands[cmdName]

    if (!cmd) {
      const err = new UnknownCommand(rawCmd, args)
      this.bufferedCommands.push(err)
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
      if (!(err instanceof UserFacedError)) {
        throw err
      }

      results.push(err)
    }

    return { response: results }
  }
}

export class MultiCommand implements Command {
  constructor(
    private readonly commands: Record<string, Command>,
    private readonly transactionalCommander: TransactionalCommander,
  ) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length) {
      throw new WrongNumberOfArguments(rawCmd.toString())
    }

    const multiCommander = new MultiCommander(
      this.transactionalCommander,
      this.commands,
    )
    this.transactionalCommander.setTransaction(multiCommander)

    return Promise.resolve({ response: 'OK' })
  }
}

export class DiscardCommand implements Command {
  constructor(
    private readonly transactionalCommander: TransactionalCommander,
  ) {}

  getKeys(): Buffer[] {
    return []
  }

  run(): Promise<CommandResult> {
    this.transactionalCommander.setTransaction(null)
    return Promise.resolve({ response: 'OK' })
  }
}

export class ExecCommand implements Command {
  constructor(
    private readonly transactionalCommander: TransactionalCommander,
    private readonly multiCommander: MultiCommander,
  ) {}

  getKeys(): Buffer[] {
    return []
  }

  async run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    try {
      if (args.length) {
        throw new TransactionDiscardedWithReson('TODO')
      }

      const res = await this.multiCommander.execBuffer()

      // TODO just check a flag instead of looping
      for (const result of res.response as CommandResult[]) {
        if (result instanceof UserFacedError) {
          throw new TransactionDiscardedWithError()
        }
      }

      return res
    } finally {
      this.transactionalCommander.setTransaction(null)
    }
  }
}

export default function createMulti(
  commands: Record<string, Command>,
  transactionalCommander: TransactionalCommander,
) {
  return new MultiCommand(commands, transactionalCommander)
}
