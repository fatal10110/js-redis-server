import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class LremCommand extends SchemaCommand<[Buffer, number, Buffer]> {
  metadata = defineCommand('lrem', {
    arity: 4, // LREM key count element
    flags: {
      write: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([t.key(), t.integer(), t.string()])

  protected execute(
    [key, count, value]: [Buffer, number, Buffer],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(0)
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    const removed = existing.lrem(count, value)
    if (existing.llen() === 0) {
      db.del(key)
    }
    transport.write(removed)
  }
}
