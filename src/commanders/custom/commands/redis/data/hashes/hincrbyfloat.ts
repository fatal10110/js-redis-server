import { ExpectedFloat, WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class HincrbyfloatCommand extends SchemaCommand<
  [Buffer, Buffer, string]
> {
  constructor(private readonly db: DB) {
    super()
  }

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
    { transport }: CommandContext,
  ) {
    const increment = parseFloat(incrementStr)
    if (Number.isNaN(increment)) {
      throw new ExpectedFloat()
    }
    const existing = this.db.get(key)
    if (existing !== null && !(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    const hash =
      existing instanceof HashDataType ? existing : new HashDataType()
    if (!(existing instanceof HashDataType)) {
      this.db.set(key, hash)
    }
    const result = hash.hincrbyfloat(field, increment)
    transport.write(Buffer.from(result.toString()))
  }
}
