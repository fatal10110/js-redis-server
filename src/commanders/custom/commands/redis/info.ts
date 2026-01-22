import { defineCommand, CommandCategory } from '../metadata'
import { SchemaCommand, CommandContext } from '../../schema/schema-command'
import { t } from '../../schema'

export class InfoCommand extends SchemaCommand<[string | undefined]> {
  metadata = defineCommand('info', {
    arity: -1, // INFO [section]
    flags: {
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SERVER],
  })

  protected schema = t.tuple([t.optional(t.string())])

  protected execute(_args: [string | undefined], ctx: CommandContext) {
    ctx.transport.write('mock info')
  }
}
