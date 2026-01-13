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
export const SremCommandDefinition: CommandDefinition = {
  metadata: defineCommand('srem', {
    arity: -3, // SREM key member [member ...]
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  }),
  factory: deps => new SremCommand(deps.db),
}

export class SremCommand implements Command {
  readonly metadata = SremCommandDefinition.metadata

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

    if (existing === null) {
      return Promise.resolve({ response: 0 })
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    let removedCount = 0
    for (let i = 1; i < args.length; i++) {
      removedCount += existing.srem(args[i])
    }

    // Remove empty set from database
    if (existing.scard() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: removedCount })
  }
}

export default function (db: DB) {
  return new SremCommand(db)
}
