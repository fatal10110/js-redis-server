import { WrongType, ExpectedFloat } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zcount', {
  arity: 4, // ZCOUNT key min max
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZcountCommandDefinition: SchemaCommandRegistration<
  [Buffer, string, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.string()]),
  handler: ([key, minStr, maxStr], { db, transport }) => {
    const min = parseFloat(minStr)
    const max = parseFloat(maxStr)

    if (Number.isNaN(min) || Number.isNaN(max)) {
      throw new ExpectedFloat()
    }

    const data = db.get(key)

    if (data === null) {
      transport.write(0)
      return
    }

    if (!(data instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    transport.write(data.zcount(min, max))
  },
}

export default function (db: DB) {
  return createSchemaCommand(ZcountCommandDefinition, { db })
}
