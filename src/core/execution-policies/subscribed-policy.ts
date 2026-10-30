import type { ExecutionPolicy } from './index'
import { lookupTableArity } from '../command-arity'
import { errors } from '../redis-error'
import { RedisResult } from '../redis-result'

const UNSUBSCRIBE_COMMANDS: ReadonlySet<string> = new Set([
  'unsubscribe',
  'punsubscribe',
  'sunsubscribe',
])

export function createSubscribedModePolicy(): ExecutionPolicy {
  return {
    name: 'subscribed-mode',
    rejectsBeforeCall: true,
    beforeExecute(plan, ctx) {
      // Redis checks the subscribed context when it reads a command, not for
      // the commands EXEC runs: `MULTI; SUBSCRIBE a; PUBLISH a x; EXEC` runs
      // the PUBLISH.
      if (ctx.transactionReplay) {
        return
      }

      const profile = ctx.executor.profile
      if (ctx.session.usesSubscribedReplyMode) {
        if (plan.definition.flags.includes('subscribed')) {
          return
        }

        // Named after the table entry the call was looked up as: from 7.0 a
        // container's `config|get`, on 6.2 (no subcommand lookup) `config`.
        const { name } = lookupTableArity(
          plan.definition,
          plan.rawArgs,
          profile,
        )
        return RedisResult.fromError(errors.subscribedContext(name, profile))
      }

      // Valkey 8.0.0 / 8.0.1 refuse (P|S)UNSUBSCRIBE on a connection with no
      // subscription of any kind, in RESP2 and RESP3 alike, right after the
      // check above (a script never gets here: they are `noscript`).
      if (
        !ctx.inScript &&
        UNSUBSCRIBE_COMMANDS.has(plan.definition.name) &&
        ctx.session.pubsubSubscriptionCount === 0 &&
        profile.has('pubsub.unsubscribe-nosub') &&
        !profile.has('pubsub.unsubscribe-nosub-dropped')
      ) {
        return RedisResult.fromError(errors.notSubscribed(plan.definition.name))
      }
    },
  }
}
