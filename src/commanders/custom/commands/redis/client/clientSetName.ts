import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'
export const commandName = 'setname'
const metadata = defineCommand(`client|${commandName}`, {
  arity: 2, // CLIENT SETNAME <name>
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.CONNECTION],
})
export const ClientSetNameCommandDefinition: SchemaCommandRegistration<
  [string]
> = {
  metadata,
  schema: t.tuple([t.string()]),
  handler: (_args, ctx) => {
    ctx.transport.write('OK')
  },
}
export default function (db: DB) {
  return createSchemaCommand(ClientSetNameCommandDefinition, { db })
}
