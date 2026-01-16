import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('srem', {
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

export const SremCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.variadic(t.string())]),
  handler: ([key, firstMember, restMembers], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return { response: 0 }
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

    return { response: removed }
  },
}

export default function (db: DB) {
  return createSchemaCommand(SremCommandDefinition, { db })
}
