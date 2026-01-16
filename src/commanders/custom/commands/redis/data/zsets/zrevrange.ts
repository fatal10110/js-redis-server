import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zrevrange', {
  arity: -4, // ZREVRANGE key start stop [WITHSCORES]
  flags: {
    readonly: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZrevrangeCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, number, 'WITHSCORES' | undefined]
> = {
  metadata,
  schema: t.tuple([
    t.key(),
    t.integer(),
    t.integer(),
    t.optional(t.literal('WITHSCORES')),
  ]),
  handler: ([key, start, stop, withScoresToken], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return []
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const withScores = withScoresToken === 'WITHSCORES'
    const result = existing.zrevrange(start, stop, withScores)
    return result
  },
}

export default function (db: DB) {
  return createSchemaCommand(ZrevrangeCommandDefinition, { db })
}
