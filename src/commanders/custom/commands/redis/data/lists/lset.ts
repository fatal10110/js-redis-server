import { WrongType, OutOfRangeIndex } from '../../../../../../core/errors'
import { ListDataType } from '../../../../data-structures/list'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class LsetCommand extends SchemaCommand<[Buffer, number, Buffer]> {
  metadata = defineCommand('lset', {
    arity: 4, // LSET key index value
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([t.key(), t.integer(), t.string()])

  protected execute(
    [key, index, value]: [Buffer, number, Buffer],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    if (existing === null) {
      throw new OutOfRangeIndex()
    }
    if (!(existing instanceof ListDataType)) {
      throw new WrongType()
    }
    const success = existing.lset(index, value)
    if (!success) {
      throw new OutOfRangeIndex()
    }
    transport.write('OK')
  }
}
