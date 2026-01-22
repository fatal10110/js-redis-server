import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class HincrbyfloatCommand extends SchemaCommand<
  [Buffer, Buffer, string]
> {
  metadata = defineCommand('hincrbyfloat', {
    arity: 4, // HINCRBYFLOAT key field increment
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  })

  protected schema = t.tuple([t.key(), t.string(), t.string()])

  protected execute(
    [key, field, incrementStr]: [Buffer, Buffer, string],
    { db, transport }: CommandContext,
  ) {
    const increment = parseFloat(incrementStr)
    if (Number.isNaN(increment)) {
      throw new ExpectedFloat()
    }
    const existing = db.get(key)
    if (existing !== null && !(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    const hash =
      existing instanceof HashDataType ? existing : new HashDataType()
    if (!(existing instanceof HashDataType)) {
      db.set(key, hash)
    }
    const result = hash.hincrbyfloat(field, increment)
    transport.write(Buffer.from(result.toString()))
  }
}
