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
export const SmoveCommandDefinition: CommandDefinition = {
  metadata: defineCommand('smove', {
    arity: 4, // SMOVE source destination member
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 1,
    keyStep: 1,
    categories: [CommandCategory.SET],
  }),
  factory: deps => new SmoveCommand(deps.db),
}

export class SmoveCommand implements Command {
  readonly metadata = SmoveCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0], args[1]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 3) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const sourceKey = args[0]
    const destKey = args[1]
    const member = args[2]

    const sourceSet = this.db.get(sourceKey)

    if (sourceSet === null) {
      return Promise.resolve({ response: 0 })
    }

    if (!(sourceSet instanceof SetDataType)) {
      throw new WrongType()
    }

    const destSet = this.db.get(destKey)
    let destination: SetDataType

    if (destSet === null) {
      destination = new SetDataType()
      this.db.set(destKey, destination)
    } else {
      if (!(destSet instanceof SetDataType)) {
        throw new WrongType()
      }
      destination = destSet
    }

    const moved = sourceSet.smove(destination, member)

    // Remove source key if set is empty
    if (sourceSet.scard() === 0) {
      this.db.del(sourceKey)
    }

    return Promise.resolve({ response: moved ? 1 : 0 })
  }
}

export default function (db: DB) {
  return new SmoveCommand(db)
}
