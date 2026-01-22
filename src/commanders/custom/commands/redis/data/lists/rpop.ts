import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class RpopCommand extends SchemaCommand<[Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('rpop', {
    arity: 2, // RPOP key
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([t.key()])

  protected execute([key]: [Buffer], { transport }: CommandContext) {
    const existing = this.db.get(key)
    if (existing === null) {
      transport.write(null)
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    const value = existing.rpop()
    if (existing.llen() === 0) {
      this.db.del(key)
    }
    transport.write(value)
  }
}
