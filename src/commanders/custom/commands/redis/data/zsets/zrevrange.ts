import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class ZrevrangeCommand extends SchemaCommand<
  [Buffer, number, number, 'WITHSCORES' | undefined]
> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('zrevrange', {
    arity: -4, // ZREVRANGE key start stop [WITHSCORES]
    flags: {
      readonly: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.ZSET],
  })

  protected schema = t.tuple([
    t.key(),
    t.integer(),
    t.integer(),
    t.optional(t.literal('WITHSCORES')),
  ])

  protected execute(
    [key, start, stop, withScoresToken]: [
      Buffer,
      number,
      number,
      'WITHSCORES' | undefined,
    ],
    { transport }: CommandContext,
  ) {
    const existing = this.db.get(key)
    if (existing === null) {
      transport.write([])
      return
    }
    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    const withScores = withScoresToken === 'WITHSCORES'
    const result = existing.zrevrange(start, stop, withScores)
    transport.write(result)
  }
}
