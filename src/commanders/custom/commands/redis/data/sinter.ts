import { WrongNumberOfArguments, WrongType } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { SetDataType } from '../../../data-structures/set'
import { DB } from '../../../db'

export class SinterCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 1) {
      throw new WrongNumberOfArguments('sinter')
    }
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 1) {
      throw new WrongNumberOfArguments('sinter')
    }

    const firstKey = args[0]
    const firstSet = this.db.get(firstKey)

    if (firstSet === null) {
      return Promise.resolve({ response: [] })
    }

    if (!(firstSet instanceof SetDataType)) {
      throw new WrongType()
    }

    const otherSets: SetDataType[] = []
    for (let i = 1; i < args.length; i++) {
      const key = args[i]
      const set = this.db.get(key)

      if (set === null) {
        // If any set doesn't exist, intersection is empty
        return Promise.resolve({ response: [] })
      }

      if (!(set instanceof SetDataType)) {
        throw new WrongType()
      }
      otherSets.push(set)
    }

    const result = firstSet.sinter(otherSets)
    return Promise.resolve({ response: result })
  }
}

export default function (db: DB) {
  return new SinterCommand(db)
}
