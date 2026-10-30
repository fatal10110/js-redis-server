import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import type { Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'

// XREAD / XREADGROUP BLOCK from a script never blocks the server (#563): up
// to Redis 7.2 the script gets a refusal, from 7.4 / Valkey 8.0 the timeout
// reply at once. Either way other clients keep being served, and a caller
// that disconnects mid-script leaves the process up.
const testRunner = new TestRunner()
const blockAllowed =
  activeProfile !== 'redis-6.2' &&
  activeProfile !== 'redis-7.0' &&
  activeProfile !== 'redis-7.2'
const legacy = activeProfile === 'redis-6.2'

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

/** `promise`, or a rejection once `ms` passed without it settling. */
function within<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  return Promise.race([
    promise,
    delay(ms).then(() => {
      throw new Error(`${what}: no reply within ${ms} ms`)
    }),
  ])
}

describe(`XREAD BLOCK from scripts (ioredis, ${testRunner.getBackendName()})`, () => {
  let base: Redis
  const clients: Redis[] = []

  before(async () => {
    base = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    for (const client of clients) client.disconnect()
    clients.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<Redis> {
    const client = base.duplicate()
    clients.push(client)
    await client.ping()
    return client
  }

  /** An empty stream (one entry, then deleted) with group `g`. */
  async function emptyStream(client: Redis): Promise<string> {
    const key = `script-stream-block:${randomKey()}`
    await client.xadd(key, '1-1', 'a', 'b')
    await client.xgroup('CREATE', key, 'g', '$')
    await client.xdel(key, '1-1')
    return key
  }

  /** What a script calling XREAD / XREADGROUP BLOCK on an empty stream gets. */
  function assertNothingRead(outcome: unknown, command: string): void {
    if (blockAllowed) {
      assert.strictEqual(outcome, null, command)
      return
    }
    assert.ok(outcome instanceof Error, command)
    assert.match(
      outcome.message,
      new RegExp(`${command} command is not allowed with BLOCK option`),
    )
  }

  const calls: [string, string][] = [
    ['xread', "'xread','block','100','streams',KEYS[1],'$'"],
    [
      'xreadgroup',
      "'xreadgroup','group','g','c','block','100','streams',KEYS[1],'>'",
    ],
  ]

  test('EVAL with BLOCK 100 leaves other clients served', async () => {
    const caller = await connect()
    const other = await connect()
    const key = await emptyStream(caller)
    for (const [command, call] of calls) {
      const outcome = caller
        .eval(`return redis.pcall(${call})`, 1, key)
        .catch((err: unknown) => err)
      await delay(150)
      assert.strictEqual(await within(other.ping(), 2000, 'PING'), 'PONG')
      assertNothingRead(await within(outcome, 2000, command), command)
      assert.strictEqual(await within(caller.ping(), 2000, 'PING'), 'PONG')
    }
  })

  test(
    'FCALL with BLOCK 100 leaves other clients served',
    { skip: legacy && 'functions are 7.0+' },
    async () => {
      const caller = await connect()
      const other = await connect()
      const key = await emptyStream(caller)
      const library = `script_stream_block_${randomKey().replace(/\W/g, '_')}`
      await caller.call(
        'FUNCTION',
        'LOAD',
        [
          `#!lua name=${library}`,
          ...calls.map(
            ([command, call]) =>
              `redis.register_function('${library}_${command}', function(KEYS) return redis.pcall(${call}) end)`,
          ),
        ].join('\n'),
      )
      try {
        for (const [command] of calls) {
          const outcome = caller
            .call('FCALL', `${library}_${command}`, 1, key)
            .catch((err: unknown) => err)
          await delay(150)
          assert.strictEqual(await within(other.ping(), 2000, 'PING'), 'PONG')
          assertNothingRead(await within(outcome, 2000, command), command)
        }
      } finally {
        await caller.call('FUNCTION', 'DELETE', library)
      }
    },
  )

  test('BLOCK 0 and a caller that disconnects leave the process up', async () => {
    const seen: unknown[] = []
    const onUnhandled = (reason: unknown) => seen.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      const other = await connect()
      const key = await emptyStream(other)
      for (const [command, call] of calls) {
        const caller = await connect()
        void caller
          .eval(`return redis.pcall(${call.replace("'100'", "'0'")})`, 1, key)
          .catch(() => {})
        await delay(50)
        caller.disconnect()
        await delay(100)
        assert.strictEqual(
          await within(other.ping(), 2000, `PING after ${command}`),
          'PONG',
        )
      }
      assert.deepStrictEqual(seen, [])
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
  })
})
