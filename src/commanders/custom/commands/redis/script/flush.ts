import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'

const metadata = defineCommand('script|flush', {
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

export const ScriptFlushCommandDefinition: SchemaCommandRegistration<
  ['ASYNC' | 'SYNC' | undefined]
> = {
  metadata,
  schema: t.tuple([t.optional(t.xor([t.literal('ASYNC'), t.literal('SYNC')]))]),
  handler: (_args, { db }) => {
    db.flushScripts()
    return 'OK'
  },
}

export default function (db: DB) {
  return createSchemaCommand(ScriptFlushCommandDefinition, { db })
}
