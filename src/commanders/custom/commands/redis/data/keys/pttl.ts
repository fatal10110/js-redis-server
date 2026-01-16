import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('pttl', {
  arity: 2, // PTTL key
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.GENERIC],
})

export const PttlCommandDefinition: SchemaCommandRegistration<[Buffer]> = {
  metadata,
  schema: t.tuple([t.key()]),
  handler: ([key], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return -2
    }

    const ttl = db.getTtl(key)
    if (ttl === -1) {
      return -1
    }

    const remainingMilliseconds = Math.max(0, ttl - Date.now())
    return remainingMilliseconds
  },
}

export default function (db: DB) {
  return createSchemaCommand(PttlCommandDefinition, { db })
}
