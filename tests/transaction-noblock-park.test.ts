import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  createNonBlockingParkHandler,
  defineCommand,
  RedisResult,
  RedisValue,
  t,
} from '../src'
import type { RedisDatabase } from '../src'
import { createRedisSessionHarness } from './core-session-test-helpers'

function buf(...tokens: string[]): Buffer[] {
  return tokens.map(token => Buffer.from(token))
}

// Count live per-key mutation-bus listeners on a database. A blocking command
// subscribes one per key before parking and must release them all; a leak shows
// up here as a non-zero count after the command finishes (issue #127).
function keyListenerCount(db: RedisDatabase): number {
  const map = (
    db as unknown as {
      mutations: { keyListeners: Map<string, Set<unknown>> }
    }
  ).mutations.keyListeners
  let total = 0
  for (const set of map.values()) {
    total += set.size
  }
  return total
}

// Blocking commands that park when their keys are empty, with an invocation
// that hits the blocking branch (all keys missing) so each one parks inside EXEC.
const blockingInvocations: ReadonlyArray<{
  name: string
  args: Buffer[]
}> = [
  { name: 'blpop', args: buf('blpop', 'nokey', '5') },
  { name: 'brpop', args: buf('brpop', 'nokey', '5') },
  { name: 'blmove', args: buf('blmove', 'src', 'dst', 'LEFT', 'RIGHT', '5') },
  { name: 'blmpop', args: buf('blmpop', '5', '1', 'nokey', 'LEFT') },
  { name: 'bzmpop', args: buf('bzmpop', '5', '1', 'nokey', 'MIN') },
  {
    name: 'xread',
    args: buf('xread', 'BLOCK', '5000', 'STREAMS', 'nostream', '$'),
  },
]

describe('blocking commands queued in MULTI/EXEC leave no listener leak (#127)', () => {
  for (const { name, args } of blockingInvocations) {
    test(`${name.toUpperCase()} in MULTI/EXEC unsubscribes its wakeup listeners`, async () => {
      const { server, session } = createRedisSessionHarness()
      const db = server.getDatabase(0)

      await session.execute('multi', buf())
      await session.execute(name, args.slice(1))
      const result = await session.execute('exec', buf())

      // EXEC must still return a single non-blocking result (not block, not error).
      assert.strictEqual(result.value.kind, 'array')
      const items = (result.value as { kind: 'array'; items: RedisValue[] })
        .items
      assert.strictEqual(items.length, 1)

      assert.strictEqual(
        keyListenerCount(db),
        0,
        `${name} must release every wakeup subscription after EXEC`,
      )
    })
  }
})

describe('createNonBlockingParkHandler', () => {
  test('resolves null (timeout sentinel) when the signal is not aborted', async () => {
    const handler = createNonBlockingParkHandler()
    const result = await handler({
      waitFor: new Promise<true>(() => {}),
      signal: new AbortController().signal,
    })
    assert.strictEqual(result, null)
  })

  test('rejects with AbortError when the signal is already aborted', async () => {
    const handler = createNonBlockingParkHandler()
    const controller = new AbortController()
    controller.abort()
    await assert.rejects(
      handler({
        waitFor: new Promise<true>(() => {}),
        signal: controller.signal,
      }),
      (err: Error) => err.name === 'AbortError',
    )
  })

  test('consumes a rejecting waitFor without emitting an unhandled rejection', async () => {
    const seen: unknown[] = []
    const onUnhandled = (reason: unknown) => seen.push(reason)
    process.on('unhandledRejection', onUnhandled)

    const handler = createNonBlockingParkHandler()
    const result = await handler({
      waitFor: Promise.reject(new Error('waitfor-boom')),
      signal: new AbortController().signal,
    })

    // Let the rejected promise reach the unhandled-rejection check.
    await new Promise(resolve => setTimeout(resolve, 50))
    process.off('unhandledRejection', onUnhandled)

    assert.strictEqual(result, null)
    assert.strictEqual(seen.length, 0, 'waitFor rejection must be swallowed')
  })
})

describe('a custom blocking command with a rejecting waitFor inside MULTI/EXEC (#127)', () => {
  // A future blocking command whose internal wait rejects must not crash the
  // process with an unhandled rejection when queued in a transaction.
  const probeReject = defineCommand({
    name: 'probereject',
    schema: t.object({}),
    flags: ['write', 'noscript'],
    keys: () => [],
    execute: async (_args, ctx) => {
      const woken = await ctx.park({
        waitFor: Promise.reject(new Error('waitfor-boom')),
        timeoutMs: undefined,
        signal: ctx.signal,
      })
      return RedisResult.create(
        RedisValue.bulkString(woken === null ? null : Buffer.from('x')),
      )
    },
  })

  test('EXEC completes without an unhandled rejection', async () => {
    const seen: unknown[] = []
    const onUnhandled = (reason: unknown) => seen.push(reason)
    process.on('unhandledRejection', onUnhandled)

    const { session } = createRedisSessionHarness({
      extraCommands: [probeReject],
    })
    await session.execute('multi', buf())
    await session.execute('probereject', buf())
    const result = await session.execute('exec', buf())

    await new Promise(resolve => setTimeout(resolve, 50))
    process.off('unhandledRejection', onUnhandled)

    assert.strictEqual(result.value.kind, 'array')
    assert.strictEqual(seen.length, 0, 'override must consume request.waitFor')
  })
})
