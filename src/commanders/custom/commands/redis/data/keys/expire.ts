import { InvalidExpireTime } from '../../../../../../core/errors'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('expire', {
  arity: 3, // EXPIRE key seconds
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.GENERIC],
})

export const ExpireCommandDefinition: SchemaCommandRegistration<
  [Buffer, number]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer()]),
  handler: ([key, seconds], { db }) => {
    if (seconds < 0) {
      throw new InvalidExpireTime(metadata.name)
    }

    if (seconds === 0) {
      const deleted = db.del(key)
      return deleted ? 1 : 0
    }

    const expiration = Date.now() + seconds * 1000
    const success = db.setExpiration(key, expiration)

    return success ? 1 : 0
  },
}

export default function (db: DB) {
  return createSchemaCommand(ExpireCommandDefinition, { db })
}
