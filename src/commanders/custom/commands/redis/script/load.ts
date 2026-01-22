import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'
import { DB } from '../../../db'

export class ScriptLoadCommand extends SchemaCommand<[Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

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

  protected execute([script]: [Buffer], { transport }: CommandContext) {
    const hash = this.db.addScript(script)
    transport.write(hash)
  }
}
