import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class ExistsCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer[]]>
{
  metadata = defineCommand('exists', {
    arity: -2, // EXISTS key [key ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: -1, // Last argument is a key
    keyStep: 1,
    categories: [CommandCategory.GENERIC],
  })

  schema = t.tuple([t.key(), t.variadic(t.key())])

  handler(
    [firstKey, restKeys]: [Buffer, Buffer[]],
    { db, transport }: SchemaCommandContext,
  ) {
    const keys = [firstKey, ...restKeys]
    let count = 0
    for (const key of keys) {
      if (db.get(key) !== null) {
        count += 1
      }
    }
    transport.write(count)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ExistsCommandDefinition(), { db })
}
