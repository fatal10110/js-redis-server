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

export class PsetexCommandDefinition
  implements SchemaCommandRegistration<[Buffer, number, string]>
{
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

  schema = t.tuple([t.key(), t.integer({ min: 1 }), t.string()])

  handler(
    [key, milliseconds, value]: [Buffer, number, string],
    { db, transport }: SchemaCommandContext,
  ) {
    if (milliseconds <= 0) {
      throw new InvalidExpireTime('psetex')
    }

    const expiration = Date.now() + milliseconds
    db.set(key, new StringDataType(Buffer.from(value)), expiration)
    transport.write('OK')
  }
}

export default function (db: DB) {
  return createSchemaCommand(new PsetexCommandDefinition(), { db })
}
