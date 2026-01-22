import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class ExistsCommand extends SchemaCommand<[Buffer, Buffer[]]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('exists', {
    arity: -2, // EXISTS key [key ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: -1, // Last argument is a key
    keyStep: 1,
    categories: [CommandCategory.GENERIC],
  })

  protected schema = t.tuple([t.key(), t.variadic(t.key())])

  protected execute(
    [firstKey, restKeys]: [Buffer, Buffer[]],
    { transport }: CommandContext,
  ) {
    const keys = [firstKey, ...restKeys]
    let count = 0
    for (const key of keys) {
      if (this.db.get(key) !== null) {
        count += 1
      }
    }
    transport.write(count)
  }
}
