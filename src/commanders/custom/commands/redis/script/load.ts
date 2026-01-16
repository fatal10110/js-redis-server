import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'
const metadata = defineCommand('script|load', {
  arity: 2, // SCRIPT LOAD <script>
  flags: {
    write: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SCRIPT],
})
export const ScriptLoadCommandDefinition: SchemaCommandRegistration<[Buffer]> =
  {
    metadata,
    schema: t.tuple([t.string()]),
    handler: ([script], { db, transport }) => {
      const hash = db.addScript(script)
      transport.write(hash)
    },
  }
export default function (db: DB) {
  return createSchemaCommand(ScriptLoadCommandDefinition, { db })
}
