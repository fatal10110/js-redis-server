import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class ZremCommand extends SchemaCommand<[Buffer, Buffer, Buffer[]]> {
  constructor(private readonly db: DB) {
    super()
  }

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
    { transport }: CommandContext,
  ) {
    const existing = this.db.get(key)
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
      this.db.del(key)
    }

    transport.write(removed)
  }
}
