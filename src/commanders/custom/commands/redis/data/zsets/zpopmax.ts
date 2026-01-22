import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class ZpopmaxCommand extends SchemaCommand<[Buffer, number?]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('zpopmax', {
    arity: -2, // ZPOPMAX key [count]
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
    { transport }: CommandContext,
  ) {
    const data = this.db.get(key)

    if (data === null) {
      transport.write([])
      return
    }

    if (!(data instanceof SortedSetDataType)) {
      throw new WrongType()
    }

    const result = data.zpopmax(count ?? 1)
    transport.write(result)
  }
}
