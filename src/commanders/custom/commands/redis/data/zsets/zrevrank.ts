import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class ZrevrankCommand extends SchemaCommand<[Buffer, Buffer]> {
  metadata = defineCommand('zrevrank', {
    arity: 3, // ZREVRANK key member
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.ZSET],
  })

  protected schema = t.tuple([t.key(), t.string()])

  protected execute(
    [key, member]: [Buffer, Buffer],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(null)
      return
    }
    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    transport.write(existing.zrevrank(member))
  }
}
