import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class ZpopminCommand extends SchemaCommand<[Buffer, number?]> {
  metadata = defineCommand('zpopmin', {
    arity: -2, // ZPOPMIN key [count]
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.ZSET],
  })

  protected schema = t.tuple([t.key(), t.optional(t.integer({ min: 1 }))])

  protected execute(
    [key, count]: [Buffer, number?],
    { db, transport }: CommandContext,
  ) {
    const data = db.get(key)

    if (data === null) {
      transport.write([])
      return
    }

    if (!(data instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const result = data.zpopmin(count ?? 1)
    transport.write(result)
  }
}
