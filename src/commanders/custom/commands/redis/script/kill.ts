import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'

export class ScriptKillCommand extends SchemaCommand<[]> {
  metadata = defineCommand('script|kill', {
    arity: 1, // SCRIPT KILL
    flags: {
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], ctx: CommandContext) {
    ctx.transport.write('OK')
  }
}
