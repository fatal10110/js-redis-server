import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class ZrangeCommand extends SchemaCommand<
  [Buffer, number, number, 'WITHSCORES' | undefined]
> {
  metadata = defineCommand('zrange', {
    arity: -4, // ZRANGE key start stop [WITHSCORES]
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
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write([])
      return
    }
    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    const withScores = withScoresToken === 'WITHSCORES'
    const result = existing.zrange(start, stop, withScores)
    transport.write(result)
  }
}
