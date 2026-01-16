import { WrongType } from '../../../../../../core/errors'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'
const metadata = defineCommand('zrem', {
  arity: -3, // ZREM key member [member ...]
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.ZSET],
})
export const ZremCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.variadic(t.string())]),
  handler: ([key, firstMember, restMembers], { db, transport }) => {
    const existing = db.get(key)
    if (existing === null) {
      {
        transport.write(0)
        return
      }
    }
    if (!(existing instanceof SortedSetDataType)) {
      throw new WrongType()
    }
    let removed = 0
    removed += existing.zrem(firstMember)
    for (const member of restMembers) {
      removed += existing.zrem(member)
    }

    if (existing.zcard() === 0) {
      db.del(key)
    }

    transport.write(removed)
  },
}
export default function (db: DB) {
  return createSchemaCommand(ZremCommandDefinition, { db })
}
