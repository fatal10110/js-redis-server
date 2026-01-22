import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class SismemberCommand extends SchemaCommand<[Buffer, Buffer]> {
  metadata = defineCommand('sismember', {
    arity: 3, // SISMEMBER key member
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  })

  protected schema = t.tuple([t.key(), t.string()])

  protected execute(
    [key, member]: [Buffer, Buffer],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(0)
      return
    }
    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }
    transport.write(existing.sismember(member) ? 1 : 0)
  }
}
