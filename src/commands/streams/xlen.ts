import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { integer } from '../helpers'

export const xlenCommand = defineCommand({
  name: 'xlen',
  schema: t.object({ key: t.key() }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const stream = ctx.db.getStream(args.key)
    return integer(stream?.entries.length ?? 0)
  },
})
