import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { StringDataType } from '../../../data-structures/string'
import { HashDataType } from '../../../data-structures/hash'
import { ListDataType } from '../../../data-structures/list'
import { SetDataType } from '../../../data-structures/set'
import { SortedSetDataType } from '../../../data-structures/zset'
import { DB } from '../../../db'

export class TypeCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('type')
    }
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('type')
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: 'none' })
    }

    if (existing instanceof StringDataType) {
      return Promise.resolve({ response: 'string' })
    } else if (existing instanceof HashDataType) {
      return Promise.resolve({ response: 'hash' })
    } else if (existing instanceof ListDataType) {
      return Promise.resolve({ response: 'list' })
    } else if (existing instanceof SetDataType) {
      return Promise.resolve({ response: 'set' })
    } else if (existing instanceof SortedSetDataType) {
      return Promise.resolve({ response: 'zset' })
    }

    return Promise.resolve({ response: 'unknown' })
  }
}

export default function (db: DB) {
  return new TypeCommand(db)
}
