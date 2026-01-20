import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class DbSizeCommandDefinition implements SchemaCommandRegistration<[]> {
  metadata = defineCommand('dbsize', {
    arity: 1, // DBSIZE
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SERVER],
  })

  schema = t.tuple([])

  handler(_args: [], { db, transport }: SchemaCommandContext) {
    const size = db.size()
    transport.write(size)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new DbSizeCommandDefinition(), { db })
}
