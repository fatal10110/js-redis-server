import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'
import { DB } from '../../../db'

export class ScriptFlushCommand extends SchemaCommand<
  ['ASYNC' | 'SYNC' | undefined]
> {
  constructor(private readonly db: DB) {
    super()
  }

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
    { transport }: CommandContext,
  ) {
    this.db.flushScripts()
    transport.write('OK')
  }
}
