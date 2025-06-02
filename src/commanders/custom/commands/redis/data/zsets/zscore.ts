import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'

export class ZscoreCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('zscore')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 2) {
      throw new WrongNumberOfArguments('zscore')
    }

    const key = args[0]
    const member = args[1]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const score = existing.zscore(member)
    const response = score !== null ? Buffer.from(score.toString()) : null
    return Promise.resolve({ response })
  }
}

export default function (db: DB) {
  return new ZscoreCommand(db)
}
