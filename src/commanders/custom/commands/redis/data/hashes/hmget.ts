import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class HmgetCommand extends SchemaCommand<[Buffer, Buffer, Buffer[]]> {
  metadata = defineCommand('hmget', {
    arity: -3, // HMGET key field [field ...]
    flags: {
      readonly: true,
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
      transport.write([null, ...restFields.map(() => null)])
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    const fields = [firstField, ...restFields]
    transport.write(existing.hmget(fields))
  }
}
