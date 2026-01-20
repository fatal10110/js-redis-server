import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../schema'

export class QuitCommandDefinition implements SchemaCommandRegistration<[]> {
  metadata = defineCommand('quit', {
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

  schema = t.tuple([])

  handler(_args: [], ctx: SchemaCommandContext) {
    ctx.transport.closeAfterFlush()
    ctx.transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new QuitCommandDefinition(), { db })
}
