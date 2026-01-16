import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('hset', {
  arity: -4, // HSET key field value [field value ...]
  flags: {
    write: true,
    denyoom: true,
    fast: true,
  },
  firstKey: 0,
  lastKey: 0,
  keyStep: 1,
  categories: [CommandCategory.HASH],
})

export const HsetCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer, Buffer, Array<[Buffer, Buffer]>]
> = {
  metadata,
  schema: t.tuple([
    t.key(),
    t.string(),
    t.string(),
    t.variadic(t.tuple([t.string(), t.string()])),
  ]),
  handler: ([key, firstField, firstValue, restPairs], { db }) => {
    const existing = db.get(key)

    if (existing !== null && !(existing instanceof HashDataType)) {
      throw new WrongType()
    }

    const hash =
      existing instanceof HashDataType ? existing : new HashDataType()

    if (!(existing instanceof HashDataType)) {
      db.set(key, hash)
    }

    let fieldsSet = 0
    fieldsSet += hash.hset(firstField, firstValue)

    for (const [field, value] of restPairs) {
      fieldsSet += hash.hset(field, value)
    }

    return { response: fieldsSet }
  },
}

export default function (db: DB) {
  return createSchemaCommand(HsetCommandDefinition, { db })
}
