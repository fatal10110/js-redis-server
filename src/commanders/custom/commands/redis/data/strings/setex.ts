import { InvalidExpireTime } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class SetexCommand extends SchemaCommand<[Buffer, number, string]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('setex', {
    arity: 4, // SETEX key seconds value
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
    [key, seconds, value]: [Buffer, number, string],
    { transport }: CommandContext,
  ) {
    if (seconds <= 0) {
      throw new InvalidExpireTime('setex')
    }

    const expiration = Date.now() + seconds * 1000
    this.db.set(key, new StringDataType(Buffer.from(value)), expiration)
    transport.write('OK')
  }
}
