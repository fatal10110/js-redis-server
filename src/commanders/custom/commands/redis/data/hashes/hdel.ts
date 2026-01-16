import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'
const metadata = defineCommand('hdel', {
  arity: -3, // HDEL key field [field ...]
  flags: {
    write: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.HASH],
})
export const HdelCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.variadic(t.string())]),
  handler: ([key, firstField, restFields], { db, transport }) => {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(0)
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    let deletedCount = 0
    deletedCount += existing.hdel(firstField)
    for (const field of restFields) {
      deletedCount += existing.hdel(field)
    }
    if (existing.hlen() === 0) {
      db.del(key)
    }
    transport.write(deletedCount)
  },
}
export default function (db: DB) {
  return createSchemaCommand(HdelCommandDefinition, { db })
}
