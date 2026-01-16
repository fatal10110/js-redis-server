import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('msetnx', {
  arity: -3, // MSETNX key value [key value ...]
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

export const MsetnxCommandDefinition: SchemaCommandRegistration<
  [Buffer, string, Array<[Buffer, string]>]
> = {
  metadata,
  schema: t.tuple([
    t.key(),
    t.string(),
    t.variadic(t.tuple([t.key(), t.string()])),
  ]),
  handler: ([firstKey, firstValue, restPairs], { db }) => {
    const pairs: Array<[Buffer, string]> = [
      [firstKey, firstValue],
      ...restPairs,
    ]

    for (const [key] of pairs) {
      if (db.get(key) !== null) {
        return 0
      }
    }

    for (const [key, value] of pairs) {
      db.set(key, new StringDataType(Buffer.from(value)))
    }

    return 1
  },
}

export default function (db: DB) {
  return createSchemaCommand(MsetnxCommandDefinition, { db })
}
