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

export class HmgetCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer, Buffer[]]>
{
  metadata = defineCommand('hmget', {
    arity: -3, // HMGET key field [field ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  })

  schema = t.tuple([t.key(), t.string(), t.variadic(t.string())])

  handler(
    [key, firstField, restFields]: [Buffer, Buffer, Buffer[]],
    { db, transport }: SchemaCommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write([null, ...restFields.map(() => null)])
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    const fields = [firstField, ...restFields]
    transport.write(existing.hmget(fields))
  }
}

export default function (db: DB) {
  return createSchemaCommand(new HmgetCommandDefinition(), { db })
}
