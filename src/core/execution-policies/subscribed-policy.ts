import type { ExecutionPolicy } from './index'
import { RedisResult } from '../redis-result'

export function createSubscribedModePolicy(): ExecutionPolicy {
  return {
    name: 'subscribed-mode',
    beforeExecute(plan, ctx) {
      if (!ctx.session.usesSubscribedReplyMode) {
        return
      }

      if (plan.flags.includes('subscribed')) {
        return
      }

      return RedisResult.error(
        `Can't execute '${plan.definition.name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`,
        'ERR',
      )
    },
  }
}
