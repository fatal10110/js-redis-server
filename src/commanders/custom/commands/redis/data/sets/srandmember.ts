import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandContext,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

export class SrandmemberCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number | undefined]>
{
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

  schema = t.tuple([t.key(), t.optional(t.integer())])

  handler(
    [key, count]: [Buffer, number | undefined],
    { db, transport }: SchemaCommandContext,
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

export default function (db: DB) {
  return createSchemaCommand(new SrandmemberCommandDefinition(), { db })
}
