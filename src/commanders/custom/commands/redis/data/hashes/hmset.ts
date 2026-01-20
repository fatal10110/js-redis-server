import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class HmsetCommandDefinition
  implements
    SchemaCommandRegistration<
      [Buffer, Buffer, Buffer, Array<[Buffer, Buffer]>]
    >
{
  metadata = defineCommand('hmset', {
    arity: -4, // HMSET key field value [field value ...]
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

  schema = t.tuple([
    t.key(),
    t.string(),
    t.string(),
    t.variadic(t.tuple([t.string(), t.string()])),
  ])

  handler(
    [key, firstField, firstValue, restPairs]: [
      Buffer,
      Buffer,
      Buffer,
      Array<[Buffer, Buffer]>,
    ],
    { db, transport }: SchemaCommandContext,
  ) {
    const existing = db.get(key)
    if (existing !== null && !(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    const hash =
      existing instanceof HashDataType ? existing : new HashDataType()
    if (!(existing instanceof HashDataType)) {
      db.set(key, hash)
    }
    hash.hset(firstField, firstValue)
    for (const [field, value] of restPairs) {
      hash.hset(field, value)
    }
    transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new HmsetCommandDefinition(), { db })
}
