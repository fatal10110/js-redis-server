import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'

const metadata = defineCommand('command', {
  arity: -1, // COMMAND [subcommand]
  flags: {
    readonly: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SERVER],
})

export const CommandInfoDefinition: SchemaCommandRegistration<[string[]]> = {
  metadata,
  schema: t.tuple([t.variadic(t.string())]),
  handler: async () => ({ response: 'mock response' }),
}

export default function (db: DB) {
  return createSchemaCommand(CommandInfoDefinition, { db })
}
