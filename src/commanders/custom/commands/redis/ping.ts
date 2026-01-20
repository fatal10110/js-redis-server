import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../schema'

export class PingCommandDefinition
  implements SchemaCommandRegistration<[string | undefined]>
{
  metadata = defineCommand('ping', {
    arity: -1, // PING [message]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  })

  schema = t.tuple([t.optional(t.string())])

  handler(_args: [string | undefined], ctx: SchemaCommandContext) {
    ctx.transport.write('PONG')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new PingCommandDefinition(), { db })
}
