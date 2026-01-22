import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class PttlCommand extends SchemaCommand<[Buffer]> {
  metadata = defineCommand('pttl', {
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
    const remainingMilliseconds = Math.max(0, ttl - Date.now())
    transport.write(remainingMilliseconds)
  }
}
