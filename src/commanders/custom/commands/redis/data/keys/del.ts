import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class DelCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer[]]>
{
  metadata = defineCommand('del', {
    arity: -2, // DEL key [key ...]
    flags: {
      write: true,
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
    let counter = 0
    for (const key of keys) {
      if (db.del(key)) {
        counter += 1
      }
    }
    transport.write(counter)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new DelCommandDefinition(), { db })
}
