import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class SpopCommand extends SchemaCommand<[Buffer]> {
  metadata = defineCommand('spop', {
    arity: 2, // SPOP key
    flags: {
      write: true,
      random: true,
      fast: true,
      noscript: true,
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
      transport.write(null)
      return
    }
    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }
    const member = existing.spop()
    if (existing.scard() === 0) {
      db.del(key)
    }
    transport.write(member)
  }
}
