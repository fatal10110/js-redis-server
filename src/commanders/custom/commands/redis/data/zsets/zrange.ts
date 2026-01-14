import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zrange', {
  arity: -4, // ZRANGE key start stop [WITHSCORES]
  flags: {
    readonly: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZrangeCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, number, 'WITHSCORES' | undefined]
> = {
  metadata,
  schema: t.tuple([
    t.key(),
    t.integer(),
    t.integer(),
    t.optional(t.literal('WITHSCORES')),
  ]),
  handler: async ([key, start, stop, withScoresToken], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: [] }
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const withScores = withScoresToken === 'WITHSCORES'
    const result = existing.zrange(start, stop, withScores)
    return { response: result }
  },
}

export default function (db: DB) {
  return createSchemaCommand(ZrangeCommandDefinition, { db })
}
