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
    schema: t.tuple([t.key()]),
    handler: async ([script], { db }) => {
      const hash = db.addScript(script)
      return { response: hash }
    },
  }

export default function (db: DB) {
  return createSchemaCommand(ScriptLoadCommandDefinition, { db })
}
