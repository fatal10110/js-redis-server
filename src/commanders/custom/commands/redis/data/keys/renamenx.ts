import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class RenamenxCommand extends SchemaCommand<[Buffer, Buffer]> {
  metadata = defineCommand('renamenx', {
    arity: 3, // RENAMENX key newkey
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 1,
    keyStep: 1,
    categories: [CommandCategory.KEYS],
  })

  protected schema = t.tuple([t.key(), t.key()])

  protected execute(
    [key, newKey]: [Buffer, Buffer],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)

    if (existing === null) {
      throw new Error('ERR no such key')
    }

    // Check if newKey already exists
    const newKeyExists = db.get(newKey)
    if (newKeyExists !== null) {
      transport.write(0)
      return
    }

    // Get the TTL before deleting
    const ttl = db.getTtl(key)
    const expiration = ttl > 0 ? ttl : undefined

    // Delete old key
    db.del(key)

    // Set new key with same value and expiration
    db.set(newKey, existing, expiration)

    transport.write(1)
  }
}
