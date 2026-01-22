import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class HgetCommand extends SchemaCommand<[Buffer, Buffer]> {
  metadata = defineCommand('hget', {
    arity: 3, // HGET key field
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  })

  protected schema = t.tuple([t.key(), t.string()])

  protected execute(
    [key, field]: [Buffer, Buffer],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(null)
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    const value = existing.hget(field)
    {
      transport.write(value)
      return
    }
  }
}
