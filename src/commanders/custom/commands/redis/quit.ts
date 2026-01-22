import { defineCommand, CommandCategory } from '../metadata'
import { SchemaCommand, CommandContext } from '../../schema/schema-command'
import { t } from '../../schema'

export class QuitCommand extends SchemaCommand<[]> {
  metadata = defineCommand('quit', {
    arity: 1, // QUIT
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], ctx: CommandContext) {
    ctx.transport.closeAfterFlush()
    ctx.transport.write('OK')
  }
}
