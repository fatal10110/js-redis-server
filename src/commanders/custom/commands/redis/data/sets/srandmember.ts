import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class SrandmemberCommand extends SchemaCommand<
  [Buffer, number | undefined]
> {
  metadata = defineCommand('srandmember', {
    arity: -2, // SRANDMEMBER key [count]
    flags: {
      readonly: true,
      random: true,
      noscript: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  })

  protected schema = t.tuple([t.key(), t.optional(t.integer())])

  protected execute(
    [key, count]: [Buffer, number | undefined],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      if (count !== undefined) {
        transport.write([])
        return
      }

      transport.write(null)
      return
    }
    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }
    if (count === undefined) {
      const member = existing.srandmember()

      transport.write(member)
      return
    }
    const members = existing.srandmember(count)
    transport.write(members)
  }
}
