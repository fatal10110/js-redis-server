import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class MgetCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer[]]>
{
  metadata = defineCommand('mget', {
    arity: -2, // MGET key [key ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  schema = t.tuple([t.key(), t.variadic(t.key())])

  handler(
    [firstKey, restKeys]: [Buffer, Buffer[]],
    { db, transport }: SchemaCommandContext,
  ) {
    const keys = [firstKey, ...restKeys]
    const res: (Buffer | null)[] = []
    for (const key of keys) {
      const val = db.get(key)
      if (!(val instanceof StringDataType)) {
        res.push(null)
        continue
      }
      res.push(val.data)
    }
    transport.write(res)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new MgetCommandDefinition(), { db })
}
