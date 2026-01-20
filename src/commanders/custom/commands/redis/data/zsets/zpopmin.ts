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

export class ZpopminCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number?]>
{
  metadata = defineCommand('zpopmin', {
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

  schema = t.tuple([t.key(), t.optional(t.integer({ min: 1 }))])

  handler(
    [key, count]: [Buffer, number?],
    { db, transport }: SchemaCommandContext,
  ) {
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
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ZpopminCommandDefinition(), { db })
}
