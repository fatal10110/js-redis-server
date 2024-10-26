import { Socket } from 'net'
import {
  NoMulti,
  TransactionDiscardedWithError,
  TransactionDiscardedWithReson,
  UserFacedError,
  WrongNumberOfArguments,
} from '../../errors'
import { Command, CommandProvider, CommandResult, Node } from '../../../types'

type BufferedCommand = {
  cmd: Command
  args: Buffer[]
  rawCmd: Buffer
}

class InTransactionCommand implements Command {
  constructor(private readonly originalCommand: Command) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return this.originalCommand.getKeys(rawCmd, args)
  }

  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    return { response: 'QUEUED' }
  }
}

class MultiCommander implements CommandProvider {
  private isErrorExists = false
  private readonly origCommander: CommandProvider

  private readonly bufferedCommands: BufferedCommand[] = []

  constructor(private readonly node: Node) {
    this.origCommander = this.node.commandExecutor
  }

  getOrCreateCommand(socket: Socket, rawCmd: Buffer, args: Buffer[]): Command {
    if (rawCmd.toString().toLowerCase() === 'exec') {
      return new ExecCommand(
        this.node,
        this.bufferedCommands,
        this.isErrorExists,
        this.origCommander,
      )
    }

    try {
      const cmd = this.origCommander.getOrCreateCommand(socket, rawCmd, args)
      // TODO "quit" command is not handled properly
      this.bufferedCommands.push({
        rawCmd,
        args,
        cmd,
      })
      return new InTransactionCommand(cmd)
    } catch (err) {
      this.isErrorExists = true
      throw err
    }
  }
}

export class MultiCommand implements Command {
  constructor(private readonly node: Node) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    const normalizedCommand = rawCmd.toString().toLowerCase()

    switch (normalizedCommand) {
      case 'multi':
        return this.runMulti(rawCmd, args)
      case 'exec':
        throw new NoMulti()
      default:
        throw new Error(`Unknown transaction command ${rawCmd.toString()}`)
    }
  }

  private runMulti(rawCmd: Buffer, args: Buffer[]): CommandResult {
    if (args.length) {
      throw new WrongNumberOfArguments(rawCmd.toString())
    }

    this.node.commandExecutor = new MultiCommander(this.node)

    return { response: 'OK' }
  }
}

export class ExecCommand implements Command {
  constructor(
    private readonly node: Node,
    private readonly commandBuffer: BufferedCommand[],
    private readonly isErrorExists: boolean,
    private readonly origCommander: CommandProvider,
  ) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    try {
      if (this.isErrorExists) {
        throw new TransactionDiscardedWithError()
      }

      if (args.length) {
        throw new TransactionDiscardedWithReson('TODO')
      }

      return this.execBuffer()
    } finally {
      this.node.commandExecutor = this.origCommander
    }
  }

  private execBuffer(): CommandResult {
    const results = []

    for (const { rawCmd, args, cmd } of this.commandBuffer) {
      try {
        const result = cmd.run(rawCmd, args)

        if (result.close) {
          return result
        }

        results.push(result.response)
      } catch (err) {
        // TODO do not repeat this logic
        if (!(err instanceof UserFacedError)) {
          throw err
        }

        results.push(err)
      }
    }

    return { response: results }
  }
}

export default function createMulti(node: Node) {
  return function () {
    return new MultiCommand(node)
  }
}
