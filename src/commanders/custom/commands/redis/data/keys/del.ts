import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class DelCommand extends SchemaCommand<[Buffer, Buffer[]]> {
  metadata = defineCommand('del', {
    arity: -2, // DEL key [key ...]
    flags: {
      write: true,
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
    let counter = 0
    for (const key of keys) {
      if (db.del(key)) {
        counter += 1
      }
    }
    transport.write(counter)
  }
}
