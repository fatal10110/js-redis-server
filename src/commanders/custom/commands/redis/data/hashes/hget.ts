import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class HgetCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer]>
{
  metadata = defineCommand('hget', {
    arity: 3, // HGET key field
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  })

  schema = t.tuple([t.key(), t.string()])

  handler(
    [key, field]: [Buffer, Buffer],
    { db, transport }: SchemaCommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(null)
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    const value = existing.hget(field)
    {
      transport.write(value)
      return
    }
  }
}

export default function (db: DB) {
  return createSchemaCommand(new HgetCommandDefinition(), { db })
}
