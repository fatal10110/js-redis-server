import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'
const metadata = defineCommand('script|kill', {
  arity: 1, // SCRIPT KILL
  flags: {
    admin: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SCRIPT],
})
export const ScriptKillCommandDefinition: SchemaCommandRegistration<[]> = {
  metadata,
  schema: t.tuple([]),
  handler: (_args, ctx) => {
    ctx.transport.write('OK')
  },
}
export default function (db: DB) {
  return createSchemaCommand(ScriptKillCommandDefinition, { db })
}
