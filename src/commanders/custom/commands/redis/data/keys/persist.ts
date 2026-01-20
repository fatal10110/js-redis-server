import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class PersistCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
  metadata = defineCommand('persist', {
    arity: 2, // PERSIST key
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.KEYS],
  })

  schema = t.tuple([t.key()])

  handler([key]: [Buffer], { db, transport }: SchemaCommandContext) {
    const result = db.persist(key)
    transport.write(result ? 1 : 0)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new PersistCommandDefinition(), { db })
}
