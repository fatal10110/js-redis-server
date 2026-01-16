import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hmget', {
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

export const HmgetCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string(), t.variadic(t.string())]),
  handler: ([key, firstField, restFields], { db }) => {
    const existing = db.get(key)

    if (existing === null) {
      return [null, ...restFields.map(() => null)]
    }

    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const fields = [firstField, ...restFields]
    return existing.hmget(fields)
  },
}

export default function (db: DB) {
  return createSchemaCommand(HmgetCommandDefinition, { db })
}
