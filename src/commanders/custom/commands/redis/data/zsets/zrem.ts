import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class ZremCommand extends SchemaCommand<[Buffer, Buffer, Buffer[]]> {
  metadata = defineCommand('zrem', {
    arity: -3, // ZREM key member [member ...]
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.ZSET],
  })

  protected schema = t.tuple([t.key(), t.string(), t.variadic(t.string())])

  protected execute(
    [key, firstMember, restMembers]: [Buffer, Buffer, Buffer[]],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      {
        transport.write(0)
        return
      }
    }
    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    let removed = 0
    removed += existing.zrem(firstMember)
    for (const member of restMembers) {
      removed += existing.zrem(member)
    }

    if (existing.zcard() === 0) {
      db.del(key)
    }

    transport.write(removed)
  }
}
