import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class GetCommand extends SchemaCommand<[Buffer]> {
  metadata = defineCommand('get', {
    arity: 2, // GET key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING, CommandCategory.GENERIC],
  })

  protected schema = t.tuple([t.key()])

  protected execute([key]: [Buffer], { db, transport }: CommandContext) {
    const val = db.get(key)
    if (val === null) {
      transport.write(null)
      return
    }
    if (!(val instanceof StringDataType)) {
      throw new WrongType()
    }
    transport.write(val.data)
  }
}
