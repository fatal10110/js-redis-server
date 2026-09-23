import type { RedisExecutionContext } from '../core/redis-context'
import type { RedisDataValue } from '../state/data-types'

export type BlockOnKeysOptions<TResult> = {
  keys: readonly Buffer[]
  /**
   * Only a write that leaves one of `keys` holding this type wakes the waiter,
   * as in real Redis (7.2+): a key overwritten with another type (`SET k foo`
   * on a key a `BLPOP` waits on) keeps the client blocked instead of waking it
   * into a WRONGTYPE reply.
   *
   * Omit it to wake on any write. `XREADGROUP` does: real Redis unblocks it
   * when its stream is overwritten with another type, and the re-run replies
   * WRONGTYPE.
   */
  type?: RedisDataValue['type']
  /** `undefined` blocks forever. */
  timeoutMs: number | undefined
  /**
   * Re-run the command's non-blocking attempt; `null` means nothing is ready
   * yet and the command keeps waiting. Errors (e.g. WRONGTYPE from another of
   * the keys) propagate, the same as real Redis re-executing the command.
   */
  attempt: () => TResult | null
}

/**
 * Park a blocking command (BLPOP, BLMOVE, BZMPOP, XREAD BLOCK, ...) until
 * `attempt` yields a result or the timeout expires (`null`).
 *
 * The key subscriptions are taken once and kept until the command finishes, so
 * a wake that finds nothing ready (another waiter won, or the value was
 * deleted in the same transaction) re-parks the command at its original place
 * in each key's listener list. Real Redis likewise keeps a blocked client's
 * position; re-subscribing would move it behind every later waiter.
 *
 * The wake is reported synchronously through `onWake`, so a turn-aware park
 * handler queues the command's resume the moment the write happens — ahead of
 * any command queued after it, in the order the waiters blocked.
 */
export async function blockOnKeys<TResult>(
  ctx: RedisExecutionContext,
  options: BlockOnKeysOptions<TResult>,
): Promise<TResult | null> {
  const { keys, type, timeoutMs, attempt } = options
  const deadline = timeoutMs === undefined ? undefined : Date.now() + timeoutMs
  // Armed only while parked: writes seen while the command is resuming or
  // re-checking are already reflected in what `attempt` observes.
  let wake: (() => void) | undefined

  const db = ctx.db
  const unsubs = keys.map(key =>
    db.subscribeKey(key, event => {
      if (event.type !== 'write') return
      if (type === undefined || event.value.type === type) wake?.()
    }),
  )

  try {
    while (true) {
      const remaining =
        deadline === undefined ? undefined : Math.max(0, deadline - Date.now())
      if (remaining === 0) return null

      const wakeListeners: Array<() => void> = []
      const waitFor = new Promise<true>(resolve => {
        wake = () => {
          wake = undefined
          resolve(true)
          for (const listener of wakeListeners) listener()
        }
      })

      let woken: true | null
      try {
        woken = await ctx.park({
          waitFor,
          onWake: listener => {
            wakeListeners.push(listener)
          },
          timeoutMs: remaining,
          signal: ctx.signal,
        })
      } finally {
        wake = undefined
      }

      if (woken === null) return null

      const result = attempt()
      if (result !== null) return result
    }
  } finally {
    for (const unsub of unsubs) {
      try {
        unsub()
      } catch {
        // ignore errors from individual unsubscribers so all are attempted
      }
    }
  }
}

/** A blocking timeout in seconds (`0` = forever) as park milliseconds. */
export function blockingTimeoutMs(timeoutSecs: number): number | undefined {
  return timeoutSecs === 0 ? undefined : Math.ceil(timeoutSecs * 1000)
}
