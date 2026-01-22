import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'

export class ScriptDebugCommand extends SchemaCommand<['YES' | 'SYNC' | 'NO']> {
  metadata = defineCommand('script|debug', {
    arity: 2, // SCRIPT DEBUG <YES|SYNC|NO>
    flags: {
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  })

  protected schema = t.tuple([
    t.xor([t.literal('YES'), t.literal('SYNC'), t.literal('NO')]),
  ])

  protected execute(_args: ['YES' | 'SYNC' | 'NO'], ctx: CommandContext) {
    ctx.transport.write('OK')
  }
}
