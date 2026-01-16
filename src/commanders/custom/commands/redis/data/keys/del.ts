import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'
const metadata = defineCommand('del', {
  arity: -2, // DEL key [key ...]
  flags: {
    write: true,
  },
  firstKey: 0,
  lastKey: -1, // Last argument is a key
  keyStep: 1,
  categories: [CommandCategory.GENERIC],
})
export const DelCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.variadic(t.key())]),
  handler: ([firstKey, restKeys], { db, transport }) => {
    const keys = [firstKey, ...restKeys]
    let counter = 0
    for (const key of keys) {
      if (db.del(key)) {
        counter += 1
      }
    }
    transport.write(counter)
  },
}
export default function (db: DB) {
  return createSchemaCommand(DelCommandDefinition, { db })
}
