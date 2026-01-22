import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class ExistsCommand extends SchemaCommand<[Buffer, Buffer[]]> {
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
    { db, transport }: CommandContext,
  ) {
    const keys = [firstKey, ...restKeys]
    let count = 0
    for (const key of keys) {
      if (db.get(key) !== null) {
        count += 1
      }
    }
    transport.write(count)
  }
}
