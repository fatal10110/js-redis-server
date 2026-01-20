import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../schema'

export class ScriptKillCommandDefinition
  implements SchemaCommandRegistration<[]>
{
  metadata = defineCommand('script|kill', {
    arity: 1, // SCRIPT KILL
    flags: {
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  })

  schema = t.tuple([])

  handler(_args: [], ctx: SchemaCommandContext) {
    ctx.transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ScriptKillCommandDefinition(), { db })
}
