import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'

export class ScriptLoadCommand extends SchemaCommand<[Buffer]> {
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

  protected schema = t.tuple([t.string()])

  protected execute([script]: [Buffer], { db, transport }: CommandContext) {
    const hash = db.addScript(script)
    transport.write(hash)
  }
}
