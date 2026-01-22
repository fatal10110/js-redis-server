import { WrongType } from '../../../../../../core/errors'
import { SetDataType } from '../../../../data-structures/set'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class SaddCommand extends SchemaCommand<[Buffer, Buffer, Buffer[]]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('sadd', {
    arity: -3, // SADD key member [member ...]
    flags: {
      write: true,
      denyoom: true,
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
    { transport }: CommandContext,
  ) {
    const existing = this.db.get(key)
    if (existing !== null && !(existing instanceof SetDataType)) {
      throw new WrongType()
    }
    const set = existing instanceof SetDataType ? existing : new SetDataType()
    if (!(existing instanceof SetDataType)) {
      this.db.set(key, set)
    }
    let added = 0
    added += set.sadd(firstMember)
    for (const member of restMembers) {
      added += set.sadd(member)
    }

    transport.write(added)
    return
  }
}
