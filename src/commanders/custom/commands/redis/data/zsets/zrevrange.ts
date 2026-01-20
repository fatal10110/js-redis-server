import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class ZrevrangeCommandDefinition
  implements
    SchemaCommandRegistration<
      [Buffer, number, number, 'WITHSCORES' | undefined]
    >
{
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

  schema = t.tuple([
    t.key(),
    t.integer(),
    t.integer(),
    t.optional(t.literal('WITHSCORES')),
  ])

  handler(
    [key, start, stop, withScoresToken]: [
      Buffer,
      number,
      number,
      'WITHSCORES' | undefined,
    ],
    { db, transport }: SchemaCommandContext,
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
    const result = existing.zrevrange(start, stop, withScores)
    transport.write(result)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ZrevrangeCommandDefinition(), { db })
}
