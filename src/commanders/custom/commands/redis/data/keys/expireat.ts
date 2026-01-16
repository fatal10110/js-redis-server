import { InvalidExpireTime } from '../../../../../../core/errors'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('expireat', {
  arity: 3, // EXPIREAT key timestamp
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.GENERIC],
})

export const ExpireatCommandDefinition: SchemaCommandRegistration<
  [Buffer, number]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer()]),
  handler: ([key, timestamp], { db }) => {
    if (timestamp < 0) {
      throw new InvalidExpireTime(metadata.name)
    }

    const expiration = timestamp * 1000
    const now = Date.now()

    if (expiration <= now) {
      const deleted = db.del(key)
      return { response: deleted ? 1 : 0 }
    }

    const success = db.setExpiration(key, expiration)

    return { response: success ? 1 : 0 }
  },
}

export default function (db: DB) {
  return createSchemaCommand(ExpireatCommandDefinition, { db })
}
