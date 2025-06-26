import {
  ExpectedInteger,
  NoScript,
  WrongNumberOfArguments,
  WrongNumberOfKeys,
} from '../../../../core/errors'
import { Command, CommandResult } from '../../../../types'
import { DB } from '../../db'

export class EvalShaCommand implements Command {
  constructor(
    private readonly evalCommand: Command,
    private readonly db: DB,
  ) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2) {
      throw new WrongNumberOfArguments('evalsha')
    }

    const keysNum = Number(args[1].toString())

    if (isNaN(keysNum)) {
      throw new ExpectedInteger()
    }

    if (args.length - 2 < keysNum) {
      throw new WrongNumberOfKeys()
    }

    const keys: Buffer[] = []

    for (let i = 0; i < keysNum; i++) {
      keys.push(args[2 + i])
    }

    return keys
  }

  run(
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<CommandResult> {
    const [bufSha, ...restArgs] = args

    if (!bufSha) {
      throw new WrongNumberOfArguments('evalsha')
    }

    const script = this.db.getScript(bufSha.toString())

    if (!script) {
      throw new NoScript()
    }

    return this.evalCommand.run(
      Buffer.from('eval'),
      [script, ...restArgs],
      signal,
    )
  }
}

export default function (evalCmd: Command, db: DB) {
  return new EvalShaCommand(evalCmd, db)
}
