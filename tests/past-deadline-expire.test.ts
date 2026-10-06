import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
  type CompatibilitySpec,
} from '../src/internal'
import type { RedisValue } from '../src/core/redis-value'

// A deadline already past (#527). Redis 6.2-8.0 and Valkey 7.2 write the key
// with SET and expire it on the next access, and delete it with `del` from
// GETEX / the EXPIRE family. Valkey 8.0 makes SET delete too (and never
// create the key); Valkey 8.1+ publishes `expired` (class `x`) for every such
// deletion. Verified against the Valkey 7.2.14 / 8.0.0 / 8.0.11 / 8.1.0 /
// 9.0.0 / 9.0.6 sources and the transcripts in #527; the Redis rows also
// against a real redis-server 7.0.15.

type Harness = {
  server: RedisServerState
  session: ClientSession
  events: string[]
  run(...command: string[]): Promise<unknown>
}

function createHarness(compatibility: CompatibilitySpec): Harness {
  const server = new RedisServerState({
    compatibility,
    activeExpiryIntervalMs: false,
  })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  const session = new ClientSession({ server, executor })
  const events: string[] = []
  server.pubsubBroker.psubscribe(Buffer.from('__keyevent@0__:*'), message => {
    const name = message.channel.toString().slice('__keyevent@0__:'.length)
    events.push(`${name} ${message.message.toString()}`)
  })
  return {
    server,
    session,
    events,
    run: (...command) => runOn(session, command),
  }
}

async function runOn(
  session: ClientSession,
  [name, ...args]: string[],
): Promise<unknown> {
  const result = await session.execute(
    name,
    args.map(arg => Buffer.from(arg)),
  )
  return plain(result.value)
}

function plain(value: RedisValue): unknown {
  switch (value.kind) {
    case 'simple-string':
      return value.value
    case 'bulk-string':
      return value.value === null ? null : value.value.toString()
    case 'integer':
      return Number(value.value)
    case 'null':
    case 'null-array':
      return null
    case 'array':
      return value.items.map(plain)
    case 'error':
      return new Error(value.message)
    default:
      throw new Error(`unexpected reply kind ${value.kind}`)
  }
}

async function withNotifications(
  harness: Harness,
  flags: string,
  body: () => Promise<void>,
): Promise<string[]> {
  assert.strictEqual(
    await harness.run('CONFIG', 'SET', 'notify-keyspace-events', flags),
    'OK',
  )
  harness.events.length = 0
  await body()
  return [...harness.events]
}

const REDIS_LIKE: CompatibilitySpec[] = [
  'redis-6.2',
  'redis-7.0',
  'redis-7.2',
  'redis-7.4',
  'redis-8.0',
  { flavor: 'valkey', version: '7.2.4' },
]

function label(spec: CompatibilitySpec): string {
  return typeof spec === 'string'
    ? spec
    : `${spec.flavor ?? 'redis'}-${spec.version ?? ''}`
}

describe('a deadline already past (#527)', () => {
  for (const spec of REDIS_LIKE) {
    describe(label(spec), () => {
      test('SET writes the key with the past TTL and publishes set, expire', async () => {
        const harness = createHarness(spec)
        const events = await withNotifications(harness, 'KEA', async () => {
          assert.strictEqual(
            await harness.run('SET', 'a', 'v', 'EXAT', '1'),
            'OK',
          )
          await harness.run('SET', 'b', 'v')
          assert.strictEqual(
            await harness.run('SET', 'b', 'w', 'PXAT', '1', 'GET'),
            'v',
          )
          // The next access expires them.
          assert.strictEqual(await harness.run('EXISTS', 'a', 'b'), 0)
        })
        assert.deepStrictEqual(events, [
          'set a',
          'expire a',
          'set b',
          'set b',
          'expire b',
          'expired a',
          'expired b',
        ])
      })

      test('GETEX and the EXPIRE family delete with del', async () => {
        const harness = createHarness(spec)
        const events = await withNotifications(harness, 'KEA', async () => {
          await deleteWithEveryPastDeadlineCommand(harness)
        })
        assert.deepStrictEqual(events, expectedEveryPastDeadline('del'))
      })
    })
  }

  describe('valkey-8.0', () => {
    test('SET never creates the key and deletes an existing one with del', async () => {
      const harness = createHarness('valkey-8.0')
      const events = await withNotifications(harness, 'KEA', async () => {
        await setWithPastDeadlines(harness)
      })
      assert.deepStrictEqual(events, ['set b', 'del b', 'rpush c', 'del c'])
    })

    test('GETEX and the EXPIRE family delete with del', async () => {
      const harness = createHarness('valkey-8.0')
      const events = await withNotifications(harness, 'KEA', async () => {
        await deleteWithEveryPastDeadlineCommand(harness)
      })
      assert.deepStrictEqual(events, expectedEveryPastDeadline('del'))
    })
  })

  describe('valkey-9.0', () => {
    test('SET never creates the key and expires an existing one', async () => {
      const harness = createHarness('valkey-9.0')
      const events = await withNotifications(harness, 'KEA', async () => {
        await setWithPastDeadlines(harness)
      })
      assert.deepStrictEqual(events, [
        'set b',
        'expired b',
        'rpush c',
        'expired c',
      ])
    })

    test('GETEX and the EXPIRE family publish expired', async () => {
      const harness = createHarness('valkey-9.0')
      const events = await withNotifications(harness, 'KEA', async () => {
        await deleteWithEveryPastDeadlineCommand(harness)
      })
      assert.deepStrictEqual(events, expectedEveryPastDeadline('expired'))
    })

    test('the expired event is class x, not g', async () => {
      const harness = createHarness('valkey-9.0')
      const run = async () => {
        await harness.run('SET', 'k', 'v')
        assert.strictEqual(await harness.run('EXPIREAT', 'k', '1'), 1)
      }
      assert.deepStrictEqual(await withNotifications(harness, 'KEg', run), [])
      assert.deepStrictEqual(await withNotifications(harness, 'KEx', run), [
        'expired k',
      ])
    })
  })

  // Valkey 8.0 writes nothing for a new key, so a WATCH on it stays clean;
  // Redis creates the key, which dirties the WATCH.
  for (const [spec, execReply] of [
    ['redis-8.0', null],
    ['valkey-8.0', []],
    ['valkey-9.0', []],
  ] as const) {
    test(`${spec}: SET EXAT past on an absent key and a WATCH on it`, async () => {
      const harness = createHarness(spec)
      const executor = createRedisCommandExecutor({
        compatibility: harness.server.profile,
      })
      const other = new ClientSession({ server: harness.server, executor })
      assert.strictEqual(await runOn(other, ['WATCH', 'k']), 'OK')
      assert.strictEqual(await harness.run('SET', 'k', 'v', 'EXAT', '1'), 'OK')
      assert.strictEqual(await runOn(other, ['MULTI']), 'OK')
      assert.deepStrictEqual(await runOn(other, ['EXEC']), execReply)
    })
  }

  for (const spec of ['valkey-8.0', 'valkey-9.0'] as const) {
    test(`${spec}: deleting an existing key dirties a WATCH on it`, async () => {
      const harness = createHarness(spec)
      const executor = createRedisCommandExecutor({
        compatibility: harness.server.profile,
      })
      const other = new ClientSession({ server: harness.server, executor })
      await harness.run('SET', 'k', 'v')
      assert.strictEqual(await runOn(other, ['WATCH', 'k']), 'OK')
      assert.strictEqual(await harness.run('SET', 'k', 'w', 'EXAT', '1'), 'OK')
      assert.strictEqual(await runOn(other, ['MULTI']), 'OK')
      assert.strictEqual(await runOn(other, ['EXEC']), null)
    })

    test(`${spec}: SET with a future deadline is unchanged`, async () => {
      const harness = createHarness(spec)
      const at = String(Math.floor(Date.now() / 1000) + 100)
      const events = await withNotifications(harness, 'KEA', async () => {
        assert.strictEqual(await harness.run('SET', 'k', 'v', 'EXAT', at), 'OK')
        assert.strictEqual(await harness.run('EXISTS', 'k'), 1)
      })
      assert.deepStrictEqual(events, ['set k', 'expire k'])
    })
  }
})

// a: absent, b: existing string, c: existing list (SET overwrites any type).
async function setWithPastDeadlines(harness: Harness): Promise<void> {
  assert.strictEqual(await harness.run('SET', 'a', 'v', 'EXAT', '1'), 'OK')
  assert.strictEqual(await harness.run('EXISTS', 'a'), 0)
  assert.strictEqual(
    await harness.run('SET', 'a', 'v', 'PXAT', '1', 'GET'),
    null,
  )
  assert.strictEqual(
    await harness.run('SET', 'a', 'v', 'NX', 'PXAT', '1'),
    'OK',
  )
  assert.strictEqual(await harness.run('EXISTS', 'a'), 0)

  await harness.run('SET', 'b', 'v')
  assert.strictEqual(
    await harness.run('SET', 'b', 'w', 'XX', 'EXAT', '1', 'GET'),
    'v',
  )
  assert.strictEqual(await harness.run('EXISTS', 'b'), 0)

  await harness.run('RPUSH', 'c', 'x')
  assert.strictEqual(await harness.run('SET', 'c', 'w', 'EXAT', '1'), 'OK')
  assert.strictEqual(await harness.run('EXISTS', 'c'), 0)
}

async function deleteWithEveryPastDeadlineCommand(
  harness: Harness,
): Promise<void> {
  for (const [key, command] of [
    ['e1', ['EXPIRE', 'e1', '-1']],
    ['e2', ['PEXPIRE', 'e2', '-5']],
    ['e3', ['EXPIREAT', 'e3', '1']],
    ['e4', ['PEXPIREAT', 'e4', '1']],
  ] as const) {
    await harness.run('SET', key, 'v')
    assert.strictEqual(await harness.run(...command), 1)
    assert.strictEqual(await harness.run('EXISTS', key), 0)
  }
  await harness.run('SET', 'g', 'v')
  assert.strictEqual(await harness.run('GETEX', 'g', 'EXAT', '1'), 'v')
  assert.strictEqual(await harness.run('EXISTS', 'g'), 0)
}

function expectedEveryPastDeadline(deletion: string): string[] {
  return ['e1', 'e2', 'e3', 'e4', 'g'].flatMap(key => [
    `set ${key}`,
    `${deletion} ${key}`,
  ])
}
