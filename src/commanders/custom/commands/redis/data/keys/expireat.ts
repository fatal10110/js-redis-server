import { InvalidExpireTime } from '../../../../../../core/errors'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class ExpireatCommand extends SchemaCommand<[Buffer, number]> {
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

  protected schema = t.tuple([t.key(), t.integer()])

  protected execute(
    [key, timestamp]: [Buffer, number],
    { db, transport }: CommandContext,
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
