import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'
const metadata = defineCommand('ttl', {
  arity: 2, // TTL key
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.GENERIC],
})
export const TtlCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: ([key], { db, transport }) => {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(-2)
      return
    }
    const ttl = db.getTtl(key)
    if (ttl === -1) {
      transport.write(-1)
      return
    }
    const remainingSeconds = Math.max(0, Math.ceil((ttl - Date.now()) / 1000))
    transport.write(remainingSeconds)
  },
}
export default function (db: DB) {
  return createSchemaCommand(TtlCommandDefinition, { db })
}
