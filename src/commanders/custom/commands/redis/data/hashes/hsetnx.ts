import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class HsetnxCommand extends SchemaCommand<[Buffer, Buffer, Buffer]> {
  metadata = defineCommand('hsetnx', {
    arity: 4, // HSETNX key field value
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

  protected schema = t.tuple([t.key(), t.string(), t.string()])

  protected execute(
    [key, field, value]: [Buffer, Buffer, Buffer],
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
    const result = hash.hsetnx(field, value)
    transport.write(result)
  }
}
