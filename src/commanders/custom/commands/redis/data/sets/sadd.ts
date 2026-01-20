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

export class SaddCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer, Buffer[]]>
{
  metadata = defineCommand('sadd', {
    arity: -3, // SADD key member [member ...]
    flags: {
      write: true,
      denyoom: true,
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
    if (existing !== null && !(existing instanceof SetDataType)) {
      throw new WrongType()
    }
    const set = existing instanceof SetDataType ? existing : new SetDataType()
    if (!(existing instanceof SetDataType)) {
      db.set(key, set)
    }
    let added = 0
    added += set.sadd(firstMember)
    for (const member of restMembers) {
      added += set.sadd(member)
    }

    transport.write(added)
    return
  }
}

export default function (db: DB) {
  return createSchemaCommand(new SaddCommandDefinition(), { db })
}
