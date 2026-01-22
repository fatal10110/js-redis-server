import { StringDataType } from '../../../../data-structures/string'
import { HashDataType } from '../../../../data-structures/hash'
import { ListDataType } from '../../../../data-structures/list'
import { SetDataType } from '../../../../data-structures/set'
import { SortedSetDataType } from '../../../../data-structures/zset'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class TypeCommand extends SchemaCommand<[Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('type', {
    arity: 2, // TYPE key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.GENERIC],
  })

  protected schema = t.tuple([t.key()])

  protected execute([key]: [Buffer], { transport }: CommandContext) {
    const existing = this.db.get(key)
    if (existing === null) {
      transport.write('none')
      return
    }
    if (existing instanceof StringDataType) {
      transport.write('string')
      return
    }
    if (existing instanceof HashDataType) {
      transport.write('hash')
      return
    }
    if (existing instanceof ListDataType) {
      transport.write('list')
      return
    }
    if (existing instanceof SetDataType) {
      transport.write('set')
      return
    }
    if (existing instanceof SortedSetDataType) {
      transport.write('zset')
      return
    }
    transport.write('unknown')
  }
}
