import {
  MovedError,
  TransactionDiscardedWithError,
  UnknownCommand,
  UserFacedError,
  WrongNumberOfArguments,
} from '../../core/errors'
import { Command, ExecutionContext, LockContext, Transport } from '../../types'
import { DB } from './db'
import { Validator } from './slot-validation'

type BufferedCommand = {
  cmd: Command
  args: Buffer[]
  rawCmd: Buffer
}

export class TransactionExecutionContext implements ExecutionContext {
  private readonly bufferedCommands: (BufferedCommand | UserFacedError)[] = []
  private shouldDiscard = false

  constructor(
    private readonly db: DB,
    private readonly originalContext: ExecutionContext,
    private readonly commands: Record<string, Command>,
    private readonly validator?: Validator,
  ) {}

  async execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
    lockContext?: LockContext,
  ): Promise<ExecutionContext> {
    const cmdName = rawCmd.toString().toLowerCase()

    if (cmdName === 'discard') {
      return this.originalContext
    }

    if (cmdName === 'exec') {
      const release = await this.db.lock.acquire()

      try {
        await this.execBuffer(transport, rawCmd, args, signal)
      } finally {
        release()
      }

      return this.originalContext
    }

    const cmd = this.commands[cmdName]

    if (!cmd) {
      const err = new UnknownCommand(rawCmd, args)
      this.bufferedCommands.push(err)
      this.shouldDiscard = true
      transport.write(err)
      return this
    }

    try {
      // TODO change to validation, currently only way to validate syntax
      cmd.getKeys(rawCmd, args)
    } catch (err) {
      if (
        err instanceof WrongNumberOfArguments ||
        err instanceof UnknownCommand
      ) {
        this.shouldDiscard = true
      }

      if (err instanceof UserFacedError) {
        transport.write(err)
        return this
      }

      throw err
    }

    try {
      this.validator?.validate(cmd, rawCmd, args)
    } catch (err) {
      if (!(err instanceof UserFacedError)) {
        throw err
      }

      transport.write(err)

      this.shouldDiscard = true

      return this
    }

    this.bufferedCommands.push({
      rawCmd,
      args,
      cmd,
    })

    transport.write('QUEUED')

    return this
  }

  shutdown(): Promise<void> {
    return Promise.resolve()
  }

  async execBuffer(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    if (this.shouldDiscard) {
      transport.write(new TransactionDiscardedWithError())
      return
    }

    const results = []
    for (const buff of this.bufferedCommands) {
      try {
        if (buff instanceof UserFacedError) {
          throw buff
        }

        const result = await buff.cmd.run(buff.rawCmd, buff.args, signal)

        if (result.close) {
          transport.write(result.response, result.close)
          return
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

    transport.write(results)
  }
}
