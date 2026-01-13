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
export const SpopCommandDefinition: CommandDefinition = {
  metadata: defineCommand('spop', {
    arity: 2, // SPOP key
    flags: {
      write: true,
      random: true,
      fast: true,
      noscript: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  }),
  factory: deps => new SpopCommand(deps.db),
}

export class SpopCommand implements Command {
  readonly metadata = SpopCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    const existing = this.db.get(key)

    if (existing === null) {
      return Promise.resolve({ response: null })
    }

    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }

    const member = existing.spop()

    // Remove key if set is empty
    if (existing.scard() === 0) {
      this.db.del(key)
    }

    return Promise.resolve({ response: member })
  }
}

export default function (db: DB) {
  return new SpopCommand(db)
}
