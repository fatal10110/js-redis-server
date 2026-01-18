import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('rename', {
  arity: 3, // RENAME key newkey
  flags: {
    write: true,
  },
  firstKey: 0,
  lastKey: 1,
  keyStep: 1,
  categories: [CommandCategory.KEYS],
})

export const RenameCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer]
> = {
  metadata,
  schema: t.tuple([t.key(), t.key()]),
  handler: ([key, newKey], { db, transport }) => {
    const existing = db.get(key)

    if (existing === null) {
      throw new Error('ERR no such key')
    }

    // Get the TTL before deleting
    const ttl = db.getTtl(key)
    const expiration = ttl > 0 ? ttl : undefined

    // Delete old key
    db.del(key)

    // Set new key with same value and expiration
    db.set(newKey, existing, expiration)

    transport.write('OK')
  },
}

export default function (db: DB) {
  return createSchemaCommand(RenameCommandDefinition, { db })
}
