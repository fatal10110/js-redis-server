import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import { createSchemaCommand, SchemaCommandRegistration, t } from '../../schema'

const metadata = defineCommand('info', {
  arity: -1, // INFO [section]
  flags: {
    readonly: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SERVER],
})

export const InfoCommandDefinition: SchemaCommandRegistration<
  [string | undefined]
> = {
  metadata,
  schema: t.tuple([t.optional(t.string())]),
  handler: () => 'mock info',
}

export default function (db: DB) {
  return createSchemaCommand(InfoCommandDefinition, { db })
}
