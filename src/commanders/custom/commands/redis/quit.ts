import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import { createSchemaCommand, SchemaCommandRegistration, t } from '../../schema'
const metadata = defineCommand('quit', {
  arity: 1, // QUIT
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.CONNECTION],
})
export const QuitCommandDefinition: SchemaCommandRegistration<[]> = {
  metadata,
  schema: t.tuple([]),
  handler: (_args, ctx) => {
    ctx.transport.closeAfterFlush()
    transport.write('OK')
  },
}
export default function (db: DB) {
  return createSchemaCommand(QuitCommandDefinition, { db })
}
