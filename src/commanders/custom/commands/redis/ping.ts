import { defineCommand, CommandCategory } from '../metadata'
import { SchemaCommand, CommandContext } from '../../schema/schema-command'
import { t } from '../../schema'

export class PingCommand extends SchemaCommand<[string | undefined]> {
  metadata = defineCommand('ping', {
    arity: -1, // PING [message]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  })

  protected schema = t.tuple([t.optional(t.string())])

  protected execute(_args: [string | undefined], ctx: CommandContext) {
    ctx.transport.write('PONG')
  }
}
