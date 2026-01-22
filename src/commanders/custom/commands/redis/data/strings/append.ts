import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class AppendCommand extends SchemaCommand<[Buffer, string]> {
  metadata = defineCommand('append', {
    arity: 3, // APPEND key value
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
    if (existing !== null && !(existing instanceof StringDataType)) {
      throw new WrongType()
    }
    const valueBuffer = Buffer.from(value)
    const newValue =
      existing instanceof StringDataType
        ? Buffer.concat([existing.data, valueBuffer])
        : valueBuffer
    db.set(key, new StringDataType(newValue))
    transport.write(newValue.length)
  }
}
