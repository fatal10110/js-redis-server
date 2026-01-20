import { DB } from '../../../db'
import { defineCommand, CommandCategory } from '../../metadata'
import {
  createSchemaCommand,
  SchemaCommandRegistration,
  SchemaCommandContext,
  t,
} from '../../../schema'

export class ScriptExistsCommandDefinition
  implements SchemaCommandRegistration<[Buffer, Buffer[]]>
{
  metadata = defineCommand('script|exists', {
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

  schema = t.tuple([t.string(), t.variadic(t.string())])

  handler(
    [firstHash, rest]: [Buffer, Buffer[]],
    { db, transport }: SchemaCommandContext,
  ) {
    const hashes = [firstHash, ...rest]
    const results = hashes.map(hash => (db.getScript(hash.toString()) ? 1 : 0))
    transport.write(results)
  }
}

export default function (db: DB) {
  return createSchemaCommand(new ScriptExistsCommandDefinition(), { db })
}
