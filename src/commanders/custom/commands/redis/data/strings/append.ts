import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class AppendCommand extends SchemaCommand<[Buffer, string]> {
  constructor(private readonly db: DB) {
    super()
  }

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
    { transport }: CommandContext,
  ) {
    const existing = this.db.get(key)
    if (existing !== null && !(existing instanceof StringDataType)) {
      throw new WrongType()
    }
    const valueBuffer = Buffer.from(value)
    const newValue =
      existing instanceof StringDataType
        ? Buffer.concat([existing.data, valueBuffer])
        : valueBuffer
    this.db.set(key, new StringDataType(newValue))
    transport.write(newValue.length)
  }
}
