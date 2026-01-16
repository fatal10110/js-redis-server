import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  t,
} from '../../../schema'

const metadata = defineCommand('script|exists', {
  arity: -2, // SCRIPT EXISTS <sha1> [<sha1> ...]
  flags: {
    readonly: true,
    fast: true,
  },
  firstKey: -1,
  lastKey: -1,
  keyStep: 1,
  categories: [CommandCategory.SCRIPT],
})

export const ScriptExistsCommandDefinition: SchemaCommandRegistration<
  [Buffer, Buffer[]]
> = {
  metadata,
  schema: t.tuple([t.string(), t.variadic(t.string())]),
  handler: ([firstHash, rest], { db }) => {
    const hashes = [firstHash, ...rest]
    const results = hashes.map(hash => (db.getScript(hash.toString()) ? 1 : 0))

    return { response: results }
  },
}

export default function (db: DB) {
  return createSchemaCommand(ScriptExistsCommandDefinition, { db })
}
