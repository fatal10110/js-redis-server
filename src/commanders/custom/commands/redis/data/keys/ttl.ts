import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class TtlCommand extends SchemaCommand<[Buffer]> {
  metadata = defineCommand('ttl', {
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

  protected schema = t.tuple([t.key()])

  protected execute([key]: [Buffer], { db, transport }: CommandContext) {
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
  }
}
