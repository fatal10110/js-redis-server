import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class FlushallCommandDefinition
  implements SchemaCommandRegistration<[]>
{
  metadata = defineCommand('flushall', {
    arity: 1, // FLUSHALL
    flags: {
      write: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.GENERIC, CommandCategory.SERVER],
  })

  schema = t.tuple([])

  handler(_args: [], { db, transport }: SchemaCommandContext) {
    db.flushall()

    transport.write('OK')
    return
  }
}

export default function (db: DB) {
  return createSchemaCommand(new FlushallCommandDefinition(), { db })
}
