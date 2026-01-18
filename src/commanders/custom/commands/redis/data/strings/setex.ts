import { InvalidExpireTime } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('setex', {
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

export const SetexCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer({ min: 1 }), t.string()]),
  handler: ([key, seconds, value], { db, transport }) => {
    if (seconds <= 0) {
      throw new InvalidExpireTime('setex')
    }

    const expiration = Date.now() + seconds * 1000
    db.set(key, new StringDataType(Buffer.from(value)), expiration)
    transport.write('OK')
  },
}

export default function (db: DB) {
  return createSchemaCommand(SetexCommandDefinition, { db })
}
