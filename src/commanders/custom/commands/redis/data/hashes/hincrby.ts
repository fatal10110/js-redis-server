import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class HincrbyCommand extends SchemaCommand<[Buffer, Buffer, number]> {
  metadata = defineCommand('hincrby', {
    arity: 4, // HINCRBY key field increment
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  })

  protected schema = t.tuple([t.key(), t.string(), t.integer()])

  protected execute(
    [key, field, increment]: [Buffer, Buffer, number],
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
    const result = hash.hincrby(field, increment)
    transport.write(result)
  }
}
