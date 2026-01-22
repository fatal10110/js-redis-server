import { InvalidExpireTime } from '../../../../../../core/errors'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class ExpireCommand extends SchemaCommand<[Buffer, number]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('expire', {
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

  protected schema = t.tuple([t.key(), t.integer()])

  protected execute(
    [key, seconds]: [Buffer, number],
    { transport }: CommandContext,
  ) {
    if (seconds < 0) {
      throw new InvalidExpireTime(this.metadata.name)
    }
    if (seconds === 0) {
      const deleted = this.db.del(key)

      transport.write(deleted ? 1 : 0)
      return
    }
    const expiration = Date.now() + seconds * 1000
    const success = this.db.setExpiration(key, expiration)
    transport.write(success ? 1 : 0)
  }
}
