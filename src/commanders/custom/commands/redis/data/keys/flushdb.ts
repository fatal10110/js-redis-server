import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class FlushdbCommandDefinition implements SchemaCommandRegistration<[]> {
  metadata = defineCommand('flushdb', {
    arity: 1, // FLUSHDB
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
    db.flushdb()
    transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new FlushdbCommandDefinition(), { db })
}
