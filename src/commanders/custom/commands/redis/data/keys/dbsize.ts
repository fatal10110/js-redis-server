import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class DbSizeCommand extends SchemaCommand<[]> {
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

  protected execute(_args: [], { db, transport }: CommandContext) {
    const size = db.size()
    transport.write(size)
  }
}
