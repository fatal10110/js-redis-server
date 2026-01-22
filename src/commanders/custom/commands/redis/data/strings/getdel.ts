import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class GetdelCommand extends SchemaCommand<[Buffer]> {
  metadata = defineCommand('getdel', {
    arity: 2, // GETDEL key
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key()])

  protected execute([key]: [Buffer], { db, transport }: CommandContext) {
    const existing = db.get(key)

    if (existing === null) {
      transport.write(null)
      return
    }

    if (!(existing instanceof StringDataType)) {
      throw new WrongType()
    }

    const value = existing.data
    db.del(key)
    transport.write(value)
  }
}
