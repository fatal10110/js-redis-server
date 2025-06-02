import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'

export class LpushCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2) {
      throw new WrongNumberOfArguments('lpush')
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2) {
      throw new WrongNumberOfArguments('lpush')
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    let list: ListDataType
    if (existing instanceof ListDataType) {
      list = existing
    } else {
      list = new ListDataType()
      this.db.set(key, list)
    }

    // Push all values in order (Redis LPUSH pushes from left to right)
    for (let i = 1; i < args.length; i++) {
      list.lpush(args[i])
    }

    return Promise.resolve({ response: list.llen() })
  }
}

export default function (db: DB) {
  return new LpushCommand(db)
}
