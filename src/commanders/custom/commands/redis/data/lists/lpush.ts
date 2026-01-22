import { WrongType } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class LpushCommand extends SchemaCommand<[Buffer, Buffer, Buffer[]]> {
  metadata = defineCommand('lpush', {
    arity: -3, // LPUSH key element [element ...]
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([t.key(), t.string(), t.variadic(t.string())])

  protected execute(
    [key, firstValue, restValues]: [Buffer, Buffer, Buffer[]],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing !== null && !(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    const list =
      existing instanceof ListDataType ? existing : new ListDataType()
    if (!(existing instanceof ListDataType)) {
      db.set(key, list)
    }
    list.lpush(firstValue)
    for (const value of restValues) {
      list.lpush(value)
    }
    transport.write(list.llen())
  }
}
