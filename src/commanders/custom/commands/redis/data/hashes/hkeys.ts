import { WrongType } from '../../../../../../core/errors'
import { HashDataType } from '../../../../data-structures/hash'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class HkeysCommand extends SchemaCommand<[Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('hkeys', {
    arity: 2, // HKEYS key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.HASH],
  })

  protected schema = t.tuple([t.key()])

  protected execute([key]: [Buffer], { transport }: CommandContext) {
    const existing = this.db.get(key)
    if (existing === null) {
      transport.write([])
      return
    }
    if (!(existing instanceof HashDataType)) {
      throw new WrongType()
    }
    transport.write(existing.hkeys())
  }
}
