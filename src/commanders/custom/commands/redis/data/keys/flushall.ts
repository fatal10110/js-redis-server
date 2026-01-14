import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('flushall', {
  arity: 1, // FLUSHALL
  flags: {
    write: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.GENERIC, CommandCategory.SERVER],
})

export const FlushallCommandDefinition: SchemaCommandRegistration<[]> = {
  metadata,
  schema: t.tuple([]),
  handler: async (_args, { db }) => {
    db.flushall()
    return { response: 'OK' }
  },
}

export default function (db: DB) {
  return createSchemaCommand(FlushallCommandDefinition, { db })
}
