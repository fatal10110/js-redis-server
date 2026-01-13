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
export const SaddCommandDefinition: CommandDefinition = {
  metadata: defineCommand('sadd', {
    arity: -3, // SADD key member [member ...]
    flags: {
      write: true,
      fast: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  }),
  factory: deps => new SaddCommand(deps.db),
}

export class SaddCommand implements Command {
  readonly metadata = SaddCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing !== null && !(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    let set: SetDataType
    if (existing instanceof SetDataType) {
      set = existing
    } else {
      set = new SetDataType()
      this.db.set(key, set)
    }

    let addedCount = 0
    for (let i = 1; i < args.length; i++) {
      addedCount += set.sadd(args[i])
    }

    return Promise.resolve({ response: addedCount })
  }
}

export default function (db: DB) {
  return new SaddCommand(db)
}
