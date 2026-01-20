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

export class HexistsCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer]>
{
  metadata = defineCommand('hexists', {
    arity: 3, // HEXISTS key field
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
      transport.write(0)
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    transport.write(existing.hexists(field) ? 1 : 0)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new HexistsCommandDefinition(), { db })
}
