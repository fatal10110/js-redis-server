import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../schema'

export class ScriptDebugCommandDefinition
  implements SchemaCommandRegistration<['YES' | 'SYNC' | 'NO']>
{
  metadata = defineCommand('script|debug', {
    arity: 2, // SCRIPT DEBUG <YES|SYNC|NO>
    flags: {
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  })

  schema = t.tuple([
    t.xor([t.literal('YES'), t.literal('SYNC'), t.literal('NO')]),
  ])

  handler(_args: ['YES' | 'SYNC' | 'NO'], ctx: SchemaCommandContext) {
    ctx.transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ScriptDebugCommandDefinition(), { db })
}
