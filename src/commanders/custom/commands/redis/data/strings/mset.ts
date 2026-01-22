import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class MsetCommand extends SchemaCommand<
  [Buffer, string, Array<[Buffer, string]>]
> {
  metadata = defineCommand('mset', {
    arity: -3, // MSET key value [key value ...]
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
    db.set(firstKey, new StringDataType(Buffer.from(firstValue)))
    for (const [key, value] of restPairs) {
      db.set(key, new StringDataType(Buffer.from(value)))
    }
    transport.write('OK')
  }
}
