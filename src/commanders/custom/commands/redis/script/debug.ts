import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'

const metadata = defineCommand('script|debug', {
  arity: 2, // SCRIPT DEBUG <YES|SYNC|NO>
  flags: {
    admin: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SCRIPT],
})

export const ScriptDebugCommandDefinition: SchemaCommandRegistration<
  ['YES' | 'SYNC' | 'NO']
> = {
  metadata,
  schema: t.tuple([
    t.xor([t.literal('YES'), t.literal('SYNC'), t.literal('NO')]),
  ]),
  handler: () => 'OK',
}

export default function (db: DB) {
  return createSchemaCommand(ScriptDebugCommandDefinition, { db })
}
