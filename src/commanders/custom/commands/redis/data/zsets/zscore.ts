import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zscore', {
  arity: 3, // ZSCORE key member
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZscoreCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string()]),
  handler: ([key, member], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: null }
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const score = existing.zscore(member)
    const response = score !== null ? Buffer.from(score.toString()) : null
    return { response }
  },
}

export default function (db: DB) {
  return createSchemaCommand(ZscoreCommandDefinition, { db })
}
