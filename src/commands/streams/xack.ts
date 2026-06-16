import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { integer } from '../helpers'
import { requireStreamGroup } from './groups'
import { parseExactId } from './ids'

export const xackCommand = defineCommand({
  name: 'xack',
  schema: t.object({
    key: t.key(),
    group: t.bulk(),
    ids: t.variadic(t.string(), { min: 1 }),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const ids = args.ids.map(parseExactId)
    requireStreamGroup(ctx.db.getStream(args.key), args.key, args.group)
    const acknowledged = ctx.db.updateStream(args.key, stream => {
      const group = requireStreamGroup(stream.value, args.key, args.group)
      return stream.ack(group, ids)
    })
    return integer(acknowledged)
  },
})
