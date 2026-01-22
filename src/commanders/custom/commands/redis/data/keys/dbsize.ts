import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class DbSizeCommand extends SchemaCommand<[]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('dbsize', {
    arity: 1, // DBSIZE
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SERVER],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], { transport }: CommandContext) {
    const size = this.db.size()
    transport.write(size)
  }
}
