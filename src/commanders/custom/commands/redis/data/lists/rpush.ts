import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('rpush', {
  arity: -3, // RPUSH key element [element ...]
  flags: {
    write: true,
    denyoom: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.LIST],
})

export const RpushCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.key(), t.key(), t.variadic(t.key())]),
  handler: async ([key, firstValue, restValues], { db }) => {
    const existing = db.get(key)

    if (existing !== null && !(existing instanceof ListDataType)) {
      throw new WrongType()
    }

    const list =
      existing instanceof ListDataType ? existing : new ListDataType()

    if (!(existing instanceof ListDataType)) {
      db.set(key, list)
    }

    list.rpush(firstValue)
    for (const value of restValues) {
      list.rpush(value)
    }

    return { response: list.llen() }
  },
}

export default function (db: DB) {
  return createSchemaCommand(RpushCommandDefinition, { db })
}
