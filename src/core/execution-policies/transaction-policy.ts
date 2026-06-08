import type { ExecutionPolicy } from './index'
import { RedisResult } from '../redis-result'
import { RedisValue } from '../redis-value'

export function createTransactionPolicy(): ExecutionPolicy {
  return {
    name: 'transaction',
    beforeExecute(plan, ctx) {
      if (ctx.session.mode !== 'transaction') {
        return
      }

      if (plan.flags.includes('transaction')) {
        return
      }

      ctx.session.queueTransaction(plan)
      return RedisResult.create(RedisValue.simpleString('QUEUED'))
    },
  }
}
