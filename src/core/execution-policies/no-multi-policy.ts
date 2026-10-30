import type { ExecutionPolicy } from './index'
import { commandTableEntry } from '../compatibility/command-table'
import { errors } from '../redis-error'
import { RedisResult } from '../redis-result'

/**
 * Refuses, inside MULTI, a command the profile's command table flags
 * `no_multi`, as `processCommand` does right after the NOAUTH check: SHUTDOWN
 * from Redis 7.0, and on Valkey 8.0+ also MULTI and WATCH (whose own nesting
 * errors are then never reached), and on Valkey 9.0 `CLIENT REPLY`. The flag is
 * read from the entry the command resolves to, `container|subcommand` from
 * 7.0 (`client|reply`), which also names the command in Valkey 9.0's error.
 * Like any queue-time refusal it dirties the transaction, so EXEC answers
 * EXECABORT. Redis 6.2 flags nothing `no_multi`.
 */
export function createNoMultiPolicy(): ExecutionPolicy {
  return {
    name: 'no-multi',
    rejectsBeforeCall: true,
    beforeExecute(plan, ctx) {
      if (ctx.session.mode !== 'transaction') {
        return
      }

      const profile = ctx.executor.profile
      const name = ctx.executor.lookupCommandName(plan.rawCommand, plan.rawArgs)
      if (
        name === null ||
        !commandTableEntry(name, profile)?.flags.includes('no_multi')
      ) {
        return
      }

      // MULTI and WATCH carry the `transaction` flag, whose errors do not
      // dirty the transaction on their own; this refusal does.
      ctx.session.markTransactionDirty()
      return RedisResult.fromError(
        errors.commandNotAllowedInTransaction(name, profile),
      )
    },
  }
}
