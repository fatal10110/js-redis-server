import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { integer } from '../helpers'
import { applyTrim, createTrimSpecSchema } from './trim'

export const xtrimCommand = defineCommand({
  name: 'xtrim',
  schema: t.object({
    key: t.key(),
    trim: createTrimSpecSchema(),
  }),
  flags: ['write'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (ctx.db.getType(args.key) === null) {
      return integer(0)
    }
    const removed = ctx.db.updateStream(args.key, stream =>
      stream.trim(value => applyTrim(value, args.trim)),
    )
    return integer(removed)
  },
})
