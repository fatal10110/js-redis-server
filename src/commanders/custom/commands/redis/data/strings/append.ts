import { WrongType } from '../../../../../../core/errors'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../../schema'
const metadata = defineCommand('append', {
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
export const AppendCommandDefinition: SchemaCommandRegistration<
  [Buffer, string]
> = {
  metadata,
  schema: t.tuple([t.key(), t.string()]),
  handler: ([key, value], { db, transport }) => {
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
  },
}
export default function (db: DB) {
  return createSchemaCommand(AppendCommandDefinition, { db })
}
