import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { bulk, ok } from './helpers'

export const pingCommand = defineCommand({
  name: 'ping',
  schema: t.object({
    message: t.optional(t.bulk()),
  }),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: args => {
    if (args.message) {
      return bulk(args.message)
    }

    return RedisResult.create(RedisValue.simpleString('PONG'))
  },
})

export const quitCommand = defineCommand({
  name: 'quit',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: () =>
    RedisResult.create(RedisValue.simpleString('OK'), { close: true }),
})

export const selectCommand = defineCommand({
  name: 'select',
  schema: t.object({
    database: t.integer({ min: 0 }),
  }),
  flags: ['fast'],
  keys: () => [],
  execute: (args, ctx) => {
    ctx.session.selectDatabase(args.database)
    return ok()
  },
})

export const connectionCommands = [pingCommand, quitCommand, selectCommand]
