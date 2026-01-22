import { defineCommand, CommandCategory } from '../../metadata'
import { SchemaCommand, CommandContext } from '../../../schema/schema-command'
import { t } from '../../../schema'
import { DB } from '../../../db'

export class ScriptExistsCommand extends SchemaCommand<[Buffer, Buffer[]]> {
  constructor(private readonly db: DB) {
    super()
  }

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

  protected schema = t.tuple([t.string(), t.variadic(t.string())])

  protected execute(
    [firstHash, rest]: [Buffer, Buffer[]],
    { transport }: CommandContext,
  ) {
    const hashes = [firstHash, ...rest]
    const results = hashes.map(hash =>
      this.db.getScript(hash.toString()) ? 1 : 0,
    )
    transport.write(results)
  }
}
