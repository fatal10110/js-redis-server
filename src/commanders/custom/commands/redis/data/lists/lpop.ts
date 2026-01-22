import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class LpopCommand extends SchemaCommand<[Buffer]> {
  metadata = defineCommand('lpop', {
    arity: 2, // LPOP key
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

  protected execute([key]: [Buffer], { db, transport }: CommandContext) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(null)
      return
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    const value = existing.lpop()
    if (existing.llen() === 0) {
      db.del(key)
    }
    transport.write(value)
  }
}
