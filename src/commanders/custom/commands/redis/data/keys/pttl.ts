import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class PttlCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
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

  schema = t.tuple([t.key()])

  handler([key]: [Buffer], { db, transport }: SchemaCommandContext) {
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

export default function (db: DB) {
  return createSchemaCommand(new PttlCommandDefinition(), { db })
}
