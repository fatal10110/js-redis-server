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

export class HstrlenCommandDefinition
  implements SchemaCommandRegistration<[Buffer, string]>
{
  metadata = defineCommand('hstrlen', {
    arity: 3, // HSTRLEN key field
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
    [key, field]: [Buffer, string],
    { db, transport }: SchemaCommandContext,
  ) {
    const data = db.get(key)

    if (data === null) {
      transport.write(0)
      return
    }

    if (!(data instanceof HashDataType)) {
      throw new WrongType()
    }

    const value = data.hget(Buffer.from(field))
    transport.write(value ? value.length : 0)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new HstrlenCommandDefinition(), { db })
}
