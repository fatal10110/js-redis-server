import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('exists', {
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

export const ExistsCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.variadic(t.key())]),
  handler: ([firstKey, restKeys], { db }) => {
    const keys = [firstKey, ...restKeys]
    let count = 0

    for (const key of keys) {
      if (db.get(key) !== null) {
        count += 1
      }
    }

    return { response: count }
  },
}

export default function (db: DB) {
  return createSchemaCommand(ExistsCommandDefinition, { db })
}
