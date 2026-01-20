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

export class ZpopmaxCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number?]>
{
  metadata = defineCommand('zpopmax', {
    arity: -2, // ZPOPMAX key [count]
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

    const result = data.zpopmax(count ?? 1)
    transport.write(result)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ZpopmaxCommandDefinition(), { db })
}
