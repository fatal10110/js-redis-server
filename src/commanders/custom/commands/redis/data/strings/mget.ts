import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('mget', {
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

export const MgetCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.variadic(t.key())]),
  handler: ([firstKey, restKeys], { db }) => {
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

    return { response: res }
  },
}

export default function (db: DB) {
  return createSchemaCommand(MgetCommandDefinition, { db })
}
