import { InvalidExpireTime } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'

export class PsetexCommand extends SchemaCommand<[Buffer, number, string]> {
  metadata = defineCommand('psetex', {
    arity: 4, // PSETEX key milliseconds value
    flags: {
      write: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key(), t.integer({ min: 1 }), t.string()])

  protected execute(
    [key, milliseconds, value]: [Buffer, number, string],
    { db, transport }: CommandContext,
  ) {
    if (milliseconds <= 0) {
      throw new InvalidExpireTime('psetex')
    }

    const expiration = Date.now() + milliseconds
    db.set(key, new StringDataType(Buffer.from(value)), expiration)
    transport.write('OK')
  }
}
