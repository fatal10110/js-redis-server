import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class MsetnxCommand extends SchemaCommand<
  [Buffer, string, Array<[Buffer, string]>]
> {
  metadata = defineCommand('msetnx', {
    arity: -3, // MSETNX key value [key value ...]
    flags: {
      write: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 2,
    limit: 2,
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([
    t.key(),
    t.string(),
    t.variadic(t.tuple([t.key(), t.string()])),
  ])

  protected execute(
    [firstKey, firstValue, restPairs]: [
      Buffer,
      string,
      Array<[Buffer, string]>,
    ],
    { db, transport }: CommandContext,
  ) {
    const pairs: Array<[Buffer, string]> = [
      [firstKey, firstValue],
      ...restPairs,
    ]
    for (const [key] of pairs) {
      if (db.get(key) !== null) {
        transport.write(0)
        return
      }
    }
    for (const [key, value] of pairs) {
      db.set(key, new StringDataType(Buffer.from(value)))
    }
    transport.write(1)
  }
}
