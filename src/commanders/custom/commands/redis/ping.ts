import { DB } from '../../db'
import { defineCommand, CommandCategory } from '../metadata'
import { createSchemaCommand, SchemaCommandRegistration, t } from '../../schema'

const metadata = defineCommand('ping', {
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

export const PingCommandDefinition: SchemaCommandRegistration<
  [string | undefined]
> = {
  metadata,
  schema: t.tuple([t.optional(t.string())]),
  handler: async () => ({ response: 'PONG' }),
}

export default function (db: DB) {
  return createSchemaCommand(PingCommandDefinition, { db })
}
