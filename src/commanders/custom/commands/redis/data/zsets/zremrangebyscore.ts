import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zremrangebyscore', {
  arity: 4, // ZREMRANGEBYSCORE key min max
  flags: {
    write: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZremrangebyscoreCommandDefinition: SchemaCommandRegistration<
  [Buffer, string, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.string()]),
  handler: async ([key, minStr, maxStr], { db }) => {
    const min = parseFloat(minStr)
    const max = parseFloat(maxStr)

    if (Number.isNaN(min) || Number.isNaN(max)) {
      throw new ExpectedFloat()
    }

    const existing = db.get(key)

    if (existing === null) {
      return { response: 0 }
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const removed = existing.zremrangebyscore(min, max)
    return { response: removed }
  },
}

export default function (db: DB) {
  return createSchemaCommand(ZremrangebyscoreCommandDefinition, { db })
}
