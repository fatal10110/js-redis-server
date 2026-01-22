import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class GetsetCommand extends SchemaCommand<[Buffer, string]> {
  metadata = defineCommand('getset', {
    arity: 3, // GETSET key value
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key(), t.string()])

  protected execute(
    [key, value]: [Buffer, string],
    { db, transport }: CommandContext,
  ) {
    const existing = db.get(key)
    let oldValue: Buffer | null = null
    if (existing !== null) {
      if (!(existing instanceof StringDataType)) {
        throw new WrongType()
      }
      oldValue = existing.data
    }
    db.set(key, new StringDataType(Buffer.from(value)))
    transport.write(oldValue)
  }
}
