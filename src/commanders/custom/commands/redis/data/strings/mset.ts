import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class MsetCommandDefinition
  implements
    SchemaCommandRegistration<[Buffer, string, Array<[Buffer, string]>]>
{
  metadata = defineCommand('mset', {
    arity: -3, // MSET key value [key value ...]
    flags: {
      write: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 2,
    limit: 2,
    categories: [CommandCategory.STRING],
  })

  schema = t.tuple([
    t.key(),
    t.string(),
    t.variadic(t.tuple([t.key(), t.string()])),
  ])

  handler(
    [firstKey, firstValue, restPairs]: [
      Buffer,
      string,
      Array<[Buffer, string]>,
    ],
    { db, transport }: SchemaCommandContext,
  ) {
    db.set(firstKey, new StringDataType(Buffer.from(firstValue)))
    for (const [key, value] of restPairs) {
      db.set(key, new StringDataType(Buffer.from(value)))
    }
    transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new MsetCommandDefinition(), { db })
}
