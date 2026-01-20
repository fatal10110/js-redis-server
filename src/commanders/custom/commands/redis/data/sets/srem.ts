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

export class SremCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer, Buffer[]]>
{
  metadata = defineCommand('srem', {
    arity: -3, // SREM key member [member ...]
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  })

  schema = t.tuple([t.key(), t.string(), t.variadic(t.string())])

  handler(
    [key, firstMember, restMembers]: [Buffer, Buffer, Buffer[]],
    { db, transport }: SchemaCommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(0)
      return
    }
    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }
    let removed = 0
    removed += existing.srem(firstMember)
    for (const member of restMembers) {
      removed += existing.srem(member)
    }
    if (existing.scard() === 0) {
      db.del(key)
    }
    transport.write(removed)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new SremCommandDefinition(), { db })
}
