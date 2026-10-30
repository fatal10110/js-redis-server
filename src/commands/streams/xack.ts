import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { integer } from '../helpers'
import { requireStreamGroup, streamGroup } from './groups'
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
    // Real Redis looks the key and the group up before it parses the ids: a
    // missing key or group acknowledges nothing, whatever the ids (#589).
    const stream = ctx.db.getStream(args.key)
    if (!stream || !streamGroup(stream, args.group)) return integer(0)
    const ids = args.ids.map(parseExactId)
    const acknowledged = ctx.db.updateStream(args.key, stream => {
      const group = requireStreamGroup(stream.value, args.key, args.group)
      return stream.ack(group, ids)
    })
    return integer(acknowledged)
  },
})
