import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../schema'

export class ScriptLoadCommandDefinition
  implements SchemaCommandRegistration<[Buffer]>
{
  metadata = defineCommand('script|load', {
    arity: 2, // SCRIPT LOAD <script>
    flags: {
      write: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  })

  schema = t.tuple([t.string()])

  handler([script]: [Buffer], { db, transport }: SchemaCommandContext) {
    const hash = db.addScript(script)
    transport.write(hash)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ScriptLoadCommandDefinition(), { db })
}
