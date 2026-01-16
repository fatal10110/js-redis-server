import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zrangebyscore', {
  arity: 4, // ZRANGEBYSCORE key min max
  flags: {
    readonly: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZrangebyscoreCommandDefinition: SchemaCommandRegistration<
  [Buffer, string, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.string()]),
  handler: ([key, minStr, maxStr], { db }) => {
    const min = parseFloat(minStr)
    const max = parseFloat(maxStr)

    if (Number.isNaN(min) || Number.isNaN(max)) {
      throw new ExpectedFloat()
    }

    const existing = db.get(key)

    if (existing === null) {
      return { response: [] }
    }

    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const result = existing.zrangebyscore(min, max)
    return { response: result }
  },
}

export default function createZrangebyscore(db: DB) {
  return createSchemaCommand(ZrangebyscoreCommandDefinition, { db })
}
