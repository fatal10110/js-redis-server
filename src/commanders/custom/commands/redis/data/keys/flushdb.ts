import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('flushdb', {
  arity: 1, // FLUSHDB
  flags: {
    write: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.GENERIC, CommandCategory.SERVER],
})

export const FlushdbCommandDefinition: SchemaCommandRegistration<[]> = {
  metadata,
  schema: t.tuple([]),
  handler: (_args, { db }) => {
    db.flushdb()
    return { response: 'OK' }
  },
}

export default function (db: DB) {
  return createSchemaCommand(FlushdbCommandDefinition, { db })
}
