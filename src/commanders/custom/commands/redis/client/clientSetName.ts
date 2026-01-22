import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'

export const commandName = 'setname'

export class ClientSetNameCommand extends SchemaCommand<[string]> {
  metadata = defineCommand(`client|${commandName}`, {
    arity: 2, // CLIENT SETNAME <name>
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CONNECTION],
  })

  protected schema = t.tuple([t.string()])

  protected execute(_args: [string], ctx: CommandContext) {
    ctx.transport.write('OK')
  }
}
