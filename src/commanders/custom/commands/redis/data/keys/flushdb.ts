import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class FlushdbCommand extends SchemaCommand<[]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('flushdb', {
    arity: 1, // FLUSHDB
    flags: {
      write: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.GENERIC, CommandCategory.SERVER],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], { transport }: CommandContext) {
    this.db.flushdb()
    transport.write('OK')
  }
}
