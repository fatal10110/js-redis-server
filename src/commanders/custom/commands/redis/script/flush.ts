import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../schema'

export class ScriptFlushCommandDefinition
  implements SchemaCommandRegistration<['ASYNC' | 'SYNC' | undefined]>
{
  metadata = defineCommand('script|flush', {
    arity: -1, // SCRIPT FLUSH [ASYNC|SYNC]
    flags: {
      write: true,
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  })

  schema = t.tuple([t.optional(t.xor([t.literal('ASYNC'), t.literal('SYNC')]))])

  handler(
    _args: ['ASYNC' | 'SYNC' | undefined],
    { db, transport }: SchemaCommandContext,
  ) {
    db.flushScripts()
    transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ScriptFlushCommandDefinition(), { db })
}
