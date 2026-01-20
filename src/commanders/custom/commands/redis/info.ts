import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../schema'

export class InfoCommandDefinition
  implements SchemaCommandRegistration<[string | undefined]>
{
  metadata = defineCommand('info', {
    arity: -1, // INFO [section]
    flags: {
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SERVER],
  })

  schema = t.tuple([t.optional(t.string())])

  handler(_args: [string | undefined], ctx: SchemaCommandContext) {
    ctx.transport.write('mock info')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new InfoCommandDefinition(), { db })
}
