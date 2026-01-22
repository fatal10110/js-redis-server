import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class SremCommand extends SchemaCommand<[Buffer, Buffer, Buffer[]]> {
  metadata = defineCommand('srem', {
    arity: -3, // SREM key member [member ...]
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.SET],
  })

  protected schema = t.tuple([t.key(), t.string(), t.variadic(t.string())])

  protected execute(
    [key, firstMember, restMembers]: [Buffer, Buffer, Buffer[]],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      transport.write(0)
      return
    }
    if (!(existing instanceof SetDataType)) {
      throw new WrongType()
    }
    let removed = 0
    removed += existing.srem(firstMember)
    for (const member of restMembers) {
      removed += existing.srem(member)
    }
    if (existing.scard() === 0) {
      db.del(key)
    }
    transport.write(removed)
  }
}
