import { InvalidExpireTime } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'

const metadata = defineCommand('psetex', {
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

export const PsetexCommandDefinition: SchemaCommandRegistration<
  [Buffer, number, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.integer({ min: 1 }), t.string()]),
  handler: ([key, milliseconds, value], { db, transport }) => {
    if (milliseconds <= 0) {
      throw new InvalidExpireTime('psetex')
    }

    const expiration = Date.now() + milliseconds
    db.set(key, new StringDataType(Buffer.from(value)), expiration)
    transport.write('OK')
  },
}

export default function (db: DB) {
  return createSchemaCommand(PsetexCommandDefinition, { db })
}
