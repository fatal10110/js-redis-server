import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class SmembersCommand extends SchemaCommand<[Buffer]> {
  metadata = defineCommand('smembers', {
    arity: 2, // SMEMBERS key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  })

  protected schema = t.tuple([t.key()])

  protected execute([key]: [Buffer], { db, transport }: CommandContext) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write([])
      return
    }
    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }
    transport.write(existing.smembers())
  }
}
