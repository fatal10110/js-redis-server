import { InvalidExpireTime } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../../schema'

export class SetexCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number, string]>
{
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

  schema = t.tuple([t.key(), t.integer({ min: 1 }), t.string()])

  handler(
    [key, seconds, value]: [Buffer, number, string],
    { db, transport }: SchemaCommandContext,
  ) {
    if (seconds <= 0) {
      throw new InvalidExpireTime('setex')
    }

    const expiration = Date.now() + seconds * 1000
    db.set(key, new StringDataType(Buffer.from(value)), expiration)
    transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new SetexCommandDefinition(), { db })
}
