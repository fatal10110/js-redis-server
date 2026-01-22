import { defineCommand, CommandCategory } from '../../../metadata'
import {
  SchemaCommand,
  CommandContext,
} from '../../../../schema/schema-command'
import { t } from '../../../../schema'
import { DB } from '../../../../db'

export class PersistCommand extends SchemaCommand<[Buffer]> {
  constructor(private readonly db: DB) {
    super()
  }

  metadata = defineCommand('persist', {
    arity: 2, // PERSIST key
    flags: {
      write: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.KEYS],
  })

  protected schema = t.tuple([t.key()])

  protected execute([key]: [Buffer], { transport }: CommandContext) {
    const result = this.db.persist(key)
    transport.write(result ? 1 : 0)
  }
}
