import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class ZincrbyCommand extends SchemaCommand<[Buffer, string, Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('zincrby', {
    arity: 4, // ZINCRBY key increment member
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.ZSET],
  })

  protected schema = t.tuple([t.key(), t.string(), t.string()])

  protected execute(
    [key, incrementStr, member]: [Buffer, string, Buffer],
    { transport }: CommandContext,
  ) {
    const increment = parseFloat(incrementStr)
    if (Number.isNaN(increment)) {
      throw new ExpectedFloat()
    }
    const existing = this.db.get(key)
    if (existing !== null && !(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    const zset =
      existing instanceof SortedSetDataType ? existing : new SortedSetDataType()
    if (!(existing instanceof SortedSetDataType)) {
      this.db.set(key, zset)
    }
    const newScore = zset.zincrby(member, increment)
    transport.write(Buffer.from(newScore.toString()))
  }
}
