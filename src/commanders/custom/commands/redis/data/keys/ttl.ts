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
  handler: async ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: -2 }
    }

    const ttl = db.getTtl(key)
    if (ttl === -1) {
      return { response: -1 }
    }

    const remainingSeconds = Math.max(0, Math.ceil((ttl - Date.now()) / 1000))
    return { response: remainingSeconds }
  },
}

export default function (db: DB) {
  return createSchemaCommand(TtlCommandDefinition, { db })
}
