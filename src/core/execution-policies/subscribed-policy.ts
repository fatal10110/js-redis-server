import type { ExecutionPolicy } from './index'
import { lookupTableArity } from '../command-arity'
import { errors } from '../redis-error'
import { RedisResult } from '../redis-result'

export function createSubscribedModePolicy(): ExecutionPolicy {
  return {
    name: 'subscribed-mode',
    rejectsBeforeCall: true,
    beforeExecute(plan, ctx) {
      // Redis checks the subscribed context when it reads a command, not for
      // the commands EXEC runs: `MULTI; SUBSCRIBE a; PUBLISH a x; EXEC` runs
      // the PUBLISH.
      if (!ctx.session.usesSubscribedReplyMode || ctx.transactionReplay) {
        return
      }

      if (plan.definition.flags.includes('subscribed')) {
        return
      }

      // Named after the table entry the call was looked up as: from 7.0 a
      // container's `config|get`, on 6.2 (no subcommand lookup) `config`.
      const profile = ctx.executor.profile
      const { name } = lookupTableArity(plan.definition, plan.rawArgs, profile)
      return RedisResult.fromError(errors.subscribedContext(name, profile))
    },
  }
}
