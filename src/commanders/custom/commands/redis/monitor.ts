import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import { createSchemaCommand, SchemaCommandRegistration, t } from '../../schema'
const metadata = defineCommand('monitor', {
  arity: 1, // MONITOR
  flags: {
    admin: true,
    blocking: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SERVER],
})
export const MonitorCommandDefinition: SchemaCommandRegistration<[]> = {
  metadata,
  schema: t.tuple([]),
  handler: (_args, ctx) => {
    {
      ctx.transport.write('OK')
      return
    }
  },
}
export default function (db: DB) {
  return createSchemaCommand(MonitorCommandDefinition, { db })
}
