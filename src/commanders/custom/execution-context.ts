import { UnknownCommand, UserFacedError } from '../../core/errors'
import { Command, ExecutionContext, Transport } from '../../types'
import { DB } from './db'
import { Validator } from './slot-validation'
import { TransactionExecutionContext } from './transaction-execution-context'

export class CommandExecutionContext implements ExecutionContext {
  constructor(
    private readonly db: DB,
    private readonly commands: Record<string, Command>,
    // TODO find a better way to pass commands to transaction execution context
    private readonly transactionCommands: Record<string, Command>,
    private readonly validator?: Validator,
  ) {}

  shutdown(): Promise<void> {
    return Promise.resolve()
  }

  async execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<ExecutionContext> {
    const cmdName = rawCmd.toString().toLowerCase()

    if (cmdName === 'multi') {
      transport.write('OK')
      return new TransactionExecutionContext(
        this.db,
        this,
        this.transactionCommands,
      )
    }

    const cmd = this.commands[cmdName]

    if (!cmd) {
      transport.write(new UnknownCommand(cmdName, args))
      return this
    }

    try {
      this.validator?.validate(cmd, rawCmd, args)
    } catch (err) {
      if (err instanceof UserFacedError) {
        transport.write(err)
        return this
      }

      throw err
    }

    try {
      const res = await cmd.run(rawCmd, args, signal)
      transport.write(res.response, res.close)
    } catch (err) {
      if (err instanceof UserFacedError) {
        transport.write(err)
        return this
      }

      throw err
    }

    return this
  }
}
