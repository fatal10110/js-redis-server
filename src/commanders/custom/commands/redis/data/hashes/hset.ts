import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class HsetCommand extends SchemaCommand<
  [Buffer, Buffer, Buffer, Array<[Buffer, Buffer]>]
> {
  metadata = defineCommand('hset', {
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

  protected schema = t.tuple([
    t.key(),
    t.string(),
    t.string(),
    t.variadic(t.tuple([t.string(), t.string()])),
  ])

  protected execute(
    [key, firstField, firstValue, restPairs]: [
      Buffer,
      Buffer,
      Buffer,
      Array<[Buffer, Buffer]>,
    ],
    { db, transport }: CommandContext,
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
    let fieldsSet = 0
    fieldsSet += hash.hset(firstField, firstValue)
    for (const [field, value] of restPairs) {
      fieldsSet += hash.hset(field, value)
    }
    transport.write(fieldsSet)
  }
}
