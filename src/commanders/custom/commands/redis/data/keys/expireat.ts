import { InvalidExpireTime } from '../../../../../../core/errors'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class ExpireatCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number]>
{
  metadata = defineCommand('expireat', {
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

  schema = t.tuple([t.key(), t.integer()])

  handler(
    [key, timestamp]: [Buffer, number],
    { db, transport }: SchemaCommandContext,
  ) {
    if (timestamp < 0) {
      throw new InvalidExpireTime(this.metadata.name)
    }
    const expiration = timestamp * 1000
    const now = Date.now()
    if (expiration <= now) {
      const deleted = db.del(key)

      transport.write(deleted ? 1 : 0)
      return
    }
    const success = db.setExpiration(key, expiration)
    transport.write(success ? 1 : 0)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ExpireatCommandDefinition(), { db })
}
