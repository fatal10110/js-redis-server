import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { popList } from './helpers'

export const lpopCommand = defineCommand({
  name: 'lpop',
  schema: t.object({
    key: t.key(),
    count: t.optional(t.integer()),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => popList(args, ctx, 'left'),
})

export const rpopCommand = defineCommand({
  name: 'rpop',
  schema: t.object({
    key: t.key(),
    count: t.optional(t.integer()),
  }),
  flags: ['write', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => popList(args, ctx, 'right'),
})
