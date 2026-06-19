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
  flags: ['readonly', 'fast', 'transaction', 'noscript'],
  capabilities: { transactionBoundary: 'begin' },
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
  flags: ['transaction', 'noscript'],
  capabilities: { transactionBoundary: 'end' },
  keys: () => [],
  execute: async (_args, ctx) => {
    if (ctx.session.mode !== 'transaction') {
      throw new ExecWithoutMultiError()
    }

    // Real Redis prioritises CLIENT_DIRTY_EXEC over CLIENT_DIRTY_CAS: a bad
    // command in the queue aborts with -EXECABORT regardless of WATCH state.
    // The (nil) CAS-abort reply is only returned when the queue itself is clean.
    if (ctx.session.isTransactionDirty()) {
      ctx.session.discardTransaction()
      throw new TransactionDiscardedError()
    }

    if (ctx.session.isWatchDirty()) {
      ctx.session.discardTransaction()
      return RedisResult.create(RedisValue.nullArray())
    }

    const plans = ctx.session.drainTransaction()
    return ctx.session.executeTransaction(plans)
  },
})

export const discardCommand = defineCommand({
  name: 'discard',
  schema: t.object({}),
  flags: ['readonly', 'fast', 'transaction', 'noscript'],
  capabilities: { transactionBoundary: 'end' },
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
  flags: ['readonly', 'fast', 'transaction', 'noscript'],
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
  flags: ['readonly', 'fast', 'noscript'],
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
