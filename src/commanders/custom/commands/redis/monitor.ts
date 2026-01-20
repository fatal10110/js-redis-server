import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../schema'

export class MonitorCommandDefinition implements SchemaCommandRegistration<[]> {
  metadata = defineCommand('monitor', {
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

  schema = t.tuple([])

  handler(_args: [], ctx: SchemaCommandContext) {
    {
      ctx.transport.write('OK')
      return
    }
  }
}

export default function (db: DB) {
  return createSchemaCommand(new MonitorCommandDefinition(), { db })
}
