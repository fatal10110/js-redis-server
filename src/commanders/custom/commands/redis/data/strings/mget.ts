import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class MgetCommand extends SchemaCommand<[Buffer, Buffer[]]> {
  metadata = defineCommand('mget', {
    arity: -2, // MGET key [key ...]
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key(), t.variadic(t.key())])

  protected execute(
    [firstKey, restKeys]: [Buffer, Buffer[]],
    { db, transport }: CommandContext,
  ) {
    const keys = [firstKey, ...restKeys]
    const res: (Buffer | null)[] = []
    for (const key of keys) {
      const val = db.get(key)
      if (!(val instanceof StringDataType)) {
        res.push(null)
        continue
      }
      res.push(val.data)
    }
    transport.write(res)
  }
}
