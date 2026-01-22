import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class HdelCommand extends SchemaCommand<[Buffer, Buffer, Buffer[]]> {
  metadata = defineCommand('hdel', {
    arity: -3, // HDEL key field [field ...]
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  })

  protected schema = t.tuple([t.key(), t.string(), t.variadic(t.string())])

  protected execute(
    [key, firstField, restFields]: [Buffer, Buffer, Buffer[]],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(0)
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    let deletedCount = 0
    deletedCount += existing.hdel(firstField)
    for (const field of restFields) {
      deletedCount += existing.hdel(field)
    }
    if (existing.hlen() === 0) {
      db.del(key)
    }
    transport.write(deletedCount)
  }
}
