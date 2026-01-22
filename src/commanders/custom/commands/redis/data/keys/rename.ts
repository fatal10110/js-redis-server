import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class RenameCommand extends SchemaCommand<[Buffer, Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('rename', {
    arity: 3, // RENAME key newkey
    flags: {
      write: true,
    },
    firstKey: 0,
    lastKey: 1,
    keyStep: 1,
    categories: [CommandCategory.KEYS],
  })

  protected schema = t.tuple([t.key(), t.key()])

  protected execute(
    [key, newKey]: [Buffer, Buffer],
    { transport }: CommandContext,
  ) {
    const existing = this.db.get(key)

    if (existing === null) {
      throw new Error('ERR no such key')
    }

    // Get the TTL before deleting
    const ttl = this.db.getTtl(key)
    const expiration = ttl > 0 ? ttl : undefined

    // Delete old key
    this.db.del(key)

    // Set new key with same value and expiration
    this.db.set(newKey, existing, expiration)

    transport.write('OK')
  }
}
