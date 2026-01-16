import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('dbsize', {
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

export const DbSizeCommandDefinition: SchemaCommandRegistration<[]> = {
  metadata,
  schema: t.tuple([]),
  handler: (_args, { db }) => {
    const size = db.size()
    return size
  },
}

export default function (db: DB) {
  return createSchemaCommand(DbSizeCommandDefinition, { db })
}
