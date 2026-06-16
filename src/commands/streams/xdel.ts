import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { integer } from '../helpers'
import { updateMaxDeletedId } from './groups'
import { compareStreamId, parseExactId } from './ids'

export const xdelCommand = defineCommand({
  name: 'xdel',
  schema: t.object({
    key: t.key(),
    ids: t.variadic(t.string(), { min: 1 }),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const targets = args.ids.map(parseExactId)
    // XDEL on a missing key returns 0 without creating the stream.
    if (ctx.db.getType(args.key) === null) {
      return integer(0)
    }

    const deleted = ctx.db.updateStream(args.key, stream => {
      let count = 0
      for (const target of targets) {
        const idx = stream.entries.findIndex(
          entry => compareStreamId(entry.id, target) === 0,
        )
        if (idx !== -1) {
          updateMaxDeletedId(stream, stream.entries[idx].id)
          stream.entries.splice(idx, 1)
          count++
        }
      }
      return count
    })
    // Streams are not removed when they become empty, matching Redis.
    return integer(deleted)
  },
})
