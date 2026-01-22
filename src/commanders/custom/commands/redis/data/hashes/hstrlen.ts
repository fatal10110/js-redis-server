import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class HstrlenCommand extends SchemaCommand<[Buffer, string]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('hstrlen', {
    arity: 3, // HSTRLEN key field
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  })

  protected schema = t.tuple([t.key(), t.string()])

  protected execute(
    [key, field]: [Buffer, string],
    { transport }: CommandContext,
  ) {
    const data = this.db.get(key)

    if (data === null) {
      transport.write(0)
      return
    }

    if (!(data instanceof HashDataType)) {
      throw new WrongType()
    }

    const value = data.hget(Buffer.from(field))
    transport.write(value ? value.length : 0)
  }
}
