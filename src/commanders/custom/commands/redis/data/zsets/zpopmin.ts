import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('zpopmin', {
  arity: -2, // ZPOPMIN key [count]
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})

export const ZpopminCommandDefinition: SchemaCommandRegistration<
  [Buffer, number?]
> = {
  metadata,
  schema: t.tuple([t.key(), t.optional(t.integer({ min: 1 }))]),
  handler: ([key, count], { db, transport }) => {
    const data = db.get(key)

    if (data === null) {
      transport.write([])
      return
    }

    if (!(data instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const result = data.zpopmin(count ?? 1)
    transport.write(result)
  },
}

export default function (db: DB) {
  return createSchemaCommand(ZpopminCommandDefinition, { db })
}
