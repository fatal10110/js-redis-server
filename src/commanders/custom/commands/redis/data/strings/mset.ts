import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('mset', {
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

export const MsetCommandDefinition: SchemaCommandRegistration<
  [Buffer, string, Array<[Buffer, string]>]
> = {
  metadata,
  schema: t.tuple([
    t.key(),
    t.string(),
    t.variadic(t.tuple([t.key(), t.string()])),
  ]),
  handler: async ([firstKey, firstValue, restPairs], { db }) => {
    db.set(firstKey, new StringDataType(Buffer.from(firstValue)))

    for (const [key, value] of restPairs) {
      db.set(key, new StringDataType(Buffer.from(value)))
    }

    return { response: 'OK' }
  },
}

export default function (db: DB) {
  return createSchemaCommand(MsetCommandDefinition, { db })
}
