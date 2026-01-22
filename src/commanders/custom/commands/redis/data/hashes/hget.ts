import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class HgetCommand extends SchemaCommand<[Buffer, Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

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
    { transport }: CommandContext,
  ) {
    const existing = this.db.get(key)
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
