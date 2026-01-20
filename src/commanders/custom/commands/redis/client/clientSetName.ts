import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../schema'

export const commandName = 'setname'

export class ClientSetNameCommandDefinition
  implements SchemaCommandRegistration<[string]>
{
  metadata = defineCommand(`client|${commandName}`, {
    arity: 2, // CLIENT SETNAME <name>
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  })

  schema = t.tuple([t.string()])

  handler(_args: [string], ctx: SchemaCommandContext) {
    ctx.transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ClientSetNameCommandDefinition(), { db })
}
