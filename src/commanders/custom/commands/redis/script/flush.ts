import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'

export class ScriptFlushCommand extends SchemaCommand<
  ['ASYNC' | 'SYNC' | undefined]
> {
  metadata = defineCommand('script|flush', {
    arity: -1, // SCRIPT FLUSH [ASYNC|SYNC]
    flags: {
      write: true,
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  })

  protected schema = t.tuple([
    t.optional(t.xor([t.literal('ASYNC'), t.literal('SYNC')])),
  ])

  protected execute(
    _args: ['ASYNC' | 'SYNC' | undefined],
    { db, transport }: CommandContext,
  ) {
    db.flushScripts()
    transport.write('OK')
  }
}
