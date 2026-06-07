import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import {
  DiscardWithoutMultiError,
  ExecWithoutMultiError,
  TransactionDiscardedError,
  WatchInsideMultiError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { ok } from './helpers'

export const multiCommand = defineCommand({
  name: 'multi',
  schema: t.object({}),
  flags: ['readonly', 'fast', 'transaction'],
  keys: () => [],
  execute: (_args, ctx) => {
    ctx.session.beginTransaction()
    return ok()
  },
})

export const execCommand = defineCommand({
  name: 'exec',
  schema: t.object({}),
  // EXEC inherits flags from queued commands at runtime. The flag list here
  // only marks it as transaction-control so policies do not treat it as a
  // normal write/read command.
  flags: ['transaction'],
  keys: () => [],
  execute: async (_args, ctx) => {
    if (ctx.session.mode !== 'transaction') {
      throw new ExecWithoutMultiError()
    }

    if (ctx.session.isWatchDirty()) {
      ctx.session.discardTransaction()
      return RedisResult.create(RedisValue.nullArray())
    }

    if (ctx.session.isTransactionDirty()) {
      ctx.session.discardTransaction()
      throw new TransactionDiscardedError()
    }

    const plans = ctx.session.drainTransaction()
    return ctx.session.executeTransaction(plans)
  },
})

export const discardCommand = defineCommand({
  name: 'discard',
  schema: t.object({}),
  flags: ['readonly', 'fast', 'transaction'],
  keys: () => [],
  execute: (_args, ctx) => {
    if (ctx.session.mode !== 'transaction') {
      throw new DiscardWithoutMultiError()
    }

    ctx.session.discardTransaction()
    return ok()
  },
})

export const watchCommand = defineCommand({
  name: 'watch',
  schema: t.object({
    keys: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['readonly', 'fast', 'transaction'],
  keys: args => args.keys,
  execute: (args, ctx) => {
    if (ctx.session.mode === 'transaction') {
      throw new WatchInsideMultiError()
    }

    ctx.session.watch(args.keys)
    return ok()
  },
})

export const unwatchCommand = defineCommand({
  name: 'unwatch',
  schema: t.object({}),
  flags: ['readonly', 'fast'],
  keys: () => [],
  execute: (_args, ctx) => {
    ctx.session.unwatch()
    return ok()
  },
})

export const transactionCommands = [
  multiCommand,
  execCommand,
  discardCommand,
  watchCommand,
  unwatchCommand,
]
