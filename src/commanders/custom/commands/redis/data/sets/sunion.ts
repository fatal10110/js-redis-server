import {
  WrongNumberOfArguments,
  WrongType,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

// Command definition with metadata
export const SunionCommandDefinition: CommandDefinition = {
  metadata: defineCommand('sunion', {
    arity: -2, // SUNION key [key ...]
    flags: {
      readonly: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SET],
  }),
  factory: deps => new SunionCommand(deps.db),
}

export class SunionCommand implements Command {
  readonly metadata = SunionCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const firstKey = args[0]
    const firstSet = this.db.get(firstKey)

    if (firstSet === null) {
      // If first set doesn't exist, start with empty set
      const emptySet = new SetDataType()
      const otherSets: SetDataType[] = []

      for (let i = 1; i < args.length; i++) {
        const key = args[i]
        const set = this.db.get(key)

        if (set !== null) {
          if (!(set instanceof SetDataType)) {
            throw new WrongType()
          }
          otherSets.push(set)
        }
      }

      const result = emptySet.sunion(otherSets)
      return Promise.resolve({ response: result })
    }

    if (!(firstSet instanceof SetDataType)) {
      throw new WrongType()
    }

    const otherSets: SetDataType[] = []
    for (let i = 1; i < args.length; i++) {
      const key = args[i]
      const set = this.db.get(key)

      if (set !== null) {
        if (!(set instanceof SetDataType)) {
          throw new WrongType()
        }
        otherSets.push(set)
      }
    }

    const result = firstSet.sunion(otherSets)
    return Promise.resolve({ response: result })
  }
}

export default function (db: DB) {
  return new SunionCommand(db)
}
