import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class SetnxCommand extends SchemaCommand<[Buffer, string]> {
  metadata = defineCommand('setnx', {
    arity: 3, // SETNX key value
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

    if (existing !== null) {
      transport.write(0)
      return
    }

    db.set(key, new StringDataType(Buffer.from(value)))
    transport.write(1)
  }
}
