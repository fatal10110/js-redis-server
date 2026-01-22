import { defineCommand, CommandCategory } from '../metadata'
import { SchemaCommand, CommandContext } from '../../schema/schema-command'
import { t } from '../../schema'

export class MonitorCommand extends SchemaCommand<[]> {
  metadata = defineCommand('monitor', {
    arity: 1, // MONITOR
    flags: {
      admin: true,
      blocking: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SERVER],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], ctx: CommandContext) {
    {
      ctx.transport.write('OK')
      return
    }
  }
}
