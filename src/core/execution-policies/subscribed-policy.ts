import type { ExecutionPolicy } from './index'
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

      return RedisResult.error(
        `Can't execute '${plan.definition.name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`,
        'ERR',
      )
    },
  }
}
