import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class StrlenCommand extends SchemaCommand<[Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('strlen', {
    arity: 2, // STRLEN key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key()])

  protected execute([key]: [Buffer], { transport }: CommandContext) {
    const val = this.db.get(key)
    if (val === null) {
      transport.write(0)
      return
    }
    if (!(val instanceof StringDataType)) {
      throw new WrongType()
    }
    transport.write(val.data.length)
  }
}
