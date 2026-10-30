import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import {
  RawRedisConnection,
  respText,
  type RespWireValue,
} from '../raw-tcp/raw-connection'

// The `m` (keymiss) and `n` (new) notify-keyspace-events classes (#514).
// `keymiss` comes from a read lookup of a missing key (`lookupKeyRead`, never
// `lookupKeyWrite`), on every profile; `new` from adding a key (`dbAdd`), from
// Redis 7.0, whose class 6.2 rejects. 7.0 also moved the sources of the store
// commands, COPY and a single-key PFCOUNT to read lookups. Captured from real
// redis-server 6.2.14, 7.0.15, 7.2.4, 8.0.0 and Valkey 8.0.0 / 9.0.0.

const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'

describe(`keymiss and new keyspace notifications (${testRunner.getBackendName()}, ${profile})`, () => {
  let actor: RawRedisConnection
  let subscriber: RawRedisConnection

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    actor = await RawRedisConnection.connect('127.0.0.1', port)
    subscriber = await RawRedisConnection.connect('127.0.0.1', port)
    subscriber.write(commandFrame('PSUBSCRIBE', '__key*@0__:*'))
    await subscriber.readFrame()
  })

  after(async () => {
    actor.close()
    subscriber.close()
    await testRunner.cleanup()
  })

  async function send(...args: string[]): Promise<string> {
    actor.write(commandFrame(...args))
    return (await actor.readRawFrame()).toString()
  }

  /**
   * Run `body` with `flags` set, then return the notifications for this
   * run's keys, in order, as `<event> <key>` (keyevent channel) or
   * `<key>: <event>` (keyspace channel), the run suffix cut off.
   */
  async function capture(
    flags: string,
    body: (key: (name: string) => string) => Promise<void>,
  ): Promise<string[]> {
    const run = randomKey()
    const key = (name: string) => `${name}:${run}`
    const sentinel = `__keyevent@0__:sentinel-${run}`
    assert.strictEqual(
      await send('CONFIG', 'SET', 'notify-keyspace-events', flags),
      '+OK\r\n',
    )
    try {
      await body(key)
    } finally {
      await send('CONFIG', 'SET', 'notify-keyspace-events', '')
    }
    await send('PUBLISH', sentinel, 'done')
    const events: string[] = []
    for (;;) {
      const frame = (await subscriber.readFrame()) as RespWireValue[]
      const channel = respText(frame[2])
      const payload = respText(frame[3])
      if (channel === sentinel) break
      if (channel.startsWith('__keyevent@0__:') && payload.endsWith(run)) {
        const event = channel.slice('__keyevent@0__:'.length)
        events.push(`${event} ${payload.slice(0, -run.length - 1)}`)
      } else if (
        channel.startsWith('__keyspace@0__:') &&
        channel.endsWith(run)
      ) {
        const name = channel.slice('__keyspace@0__:'.length, -run.length - 1)
        events.push(`${name}: ${payload}`)
      }
    }
    return events
  }

  const newKeys = (...keys: string[]): string[] =>
    legacy ? [] : keys.map(key => `new ${key}`)

  test('a read lookup of a missing key publishes keymiss, a write lookup does not', async () => {
    const events = await capture('Em', async key => {
      assert.strictEqual(await send('GET', key('get')), '$-1\r\n')
      assert.strictEqual(
        await send('MGET', key('mget1'), key('mget2')),
        '*2\r\n$-1\r\n$-1\r\n',
      )
      assert.strictEqual(await send('EXISTS', key('exists')), ':0\r\n')
      assert.strictEqual(await send('TYPE', key('type')), '+none\r\n')
      assert.strictEqual(await send('TTL', key('ttl')), ':-2\r\n')
      assert.strictEqual(await send('HGET', key('hget'), 'f'), '$-1\r\n')
      assert.strictEqual(await send('GETDEL', key('getdel')), '$-1\r\n')
      assert.strictEqual(
        await send('XREAD', 'STREAMS', key('xread'), '0'),
        '*-1\r\n',
      )
      // Write lookups: nothing.
      assert.strictEqual(await send('EXPIRE', key('expire'), '10'), ':0\r\n')
      assert.strictEqual(await send('DEL', key('del')), ':0\r\n')
      assert.strictEqual(await send('LPOP', key('lpop')), '$-1\r\n')
      assert.strictEqual(
        await send('LMOVE', key('lmove'), key('lmove-dst'), 'LEFT', 'RIGHT'),
        '$-1\r\n',
      )
      assert.strictEqual(await send('WATCH', key('watch')), '+OK\r\n')
      assert.strictEqual(await send('UNWATCH'), '+OK\r\n')
    })
    assert.deepStrictEqual(events, [
      'keymiss get',
      'keymiss mget1',
      'keymiss mget2',
      'keymiss exists',
      'keymiss type',
      'keymiss ttl',
      'keymiss hget',
      'keymiss getdel',
      // Looked up while parsing the IDs, then again while serving them.
      'keymiss xread',
      'keymiss xread',
    ])
  })

  test('scripts and EXEC publish keymiss as the command runs', async () => {
    const events = await capture('Em', async key => {
      assert.strictEqual(
        await send(
          'EVAL',
          "return redis.call('GET', KEYS[1])",
          '1',
          key('eval'),
        ),
        '$-1\r\n',
      )
      assert.strictEqual(await send('MULTI'), '+OK\r\n')
      assert.strictEqual(await send('GET', key('exec')), '+QUEUED\r\n')
      assert.strictEqual(await send('EXEC'), '*1\r\n$-1\r\n')
    })
    assert.deepStrictEqual(events, ['keymiss eval', 'keymiss exec'])
  })

  test('new is published when a key is added, not when it is overwritten', async () => {
    const events = await capture(legacy ? 'Em' : 'Emn', async key => {
      assert.strictEqual(await send('SET', key('set'), 'v'), '+OK\r\n')
      assert.strictEqual(await send('SET', key('set'), 'w'), '+OK\r\n')
      assert.strictEqual(await send('SETNX', key('set'), 'x'), ':0\r\n')
      assert.strictEqual(await send('HSET', key('hash'), 'f', 'v'), ':1\r\n')
      assert.strictEqual(await send('INCR', key('incr')), ':1\r\n')
      assert.strictEqual(
        await send('MSET', key('mset1'), 'a', key('mset2'), 'b'),
        '+OK\r\n',
      )
      // GETSET and SET ... GET read the old value first.
      assert.strictEqual(await send('GETSET', key('getset'), 'v'), '$-1\r\n')
      assert.strictEqual(
        await send('SET', key('setget'), 'v', 'GET'),
        '$-1\r\n',
      )
      // A list emptied by a pop is deleted, so the next push adds it again.
      assert.strictEqual(await send('RPUSH', key('list'), 'a'), ':1\r\n')
      assert.strictEqual(await send('LPOP', key('list')), '$1\r\na\r\n')
      assert.strictEqual(await send('RPUSH', key('list'), 'b'), ':1\r\n')
    })
    assert.deepStrictEqual(events, [
      ...newKeys('set', 'hash', 'incr', 'mset1', 'mset2'),
      'keymiss getset',
      ...newKeys('getset'),
      'keymiss setget',
      ...newKeys('setget', 'list', 'list'),
    ])
  })

  test('RENAME and COPY add their destination, even over an existing key', async () => {
    const events = await capture(legacy ? 'Em' : 'Emn', async key => {
      await send('SET', key('src'), 'v')
      await send('SET', key('taken'), 'w')
      assert.strictEqual(
        await send('RENAME', key('src'), key('dst')),
        '+OK\r\n',
      )
      assert.strictEqual(
        await send('RENAME', key('dst'), key('taken')),
        '+OK\r\n',
      )
      assert.strictEqual(
        await send('COPY', key('taken'), key('copy')),
        ':1\r\n',
      )
      assert.strictEqual(
        await send('COPY', key('taken'), key('copy'), 'REPLACE'),
        ':1\r\n',
      )
      assert.strictEqual(
        await send('COPY', key('missing'), key('copy2')),
        ':0\r\n',
      )
    })
    assert.deepStrictEqual(events, [
      ...newKeys('src', 'taken', 'dst', 'taken', 'copy', 'copy'),
      // 6.2 looks COPY's source up for writing.
      ...(legacy ? [] : ['keymiss missing']),
    ])
  })

  test('BITFIELD reads, the XACK family, SORT patterns and HGETEX', async () => {
    const hgetex = profile === 'redis-8.0' || profile === 'valkey-9.0'
    const events = await capture('Em', async key => {
      // Only GET / OVERFLOW operations: a read lookup.
      assert.strictEqual(
        await send('BITFIELD', key('bf'), 'GET', 'u8', '0'),
        '*1\r\n:0\r\n',
      )
      assert.strictEqual(
        await send(
          'BITFIELD',
          key('bf-wrap'),
          'OVERFLOW',
          'WRAP',
          'GET',
          'u8',
          '0',
        ),
        '*1\r\n:0\r\n',
      )
      assert.strictEqual(
        await send('BITFIELD', key('bf-set'), 'SET', 'u8', '0', '1'),
        '*1\r\n:0\r\n',
      )
      // The XACK reply on a missing stream is #589; only the event is checked.
      await send('XACK', key('xack'), 'g', '0-1')
      assert.match(
        await send('XCLAIM', key('xclaim'), 'g', 'c', '0', '0-1'),
        /^-NOGROUP /,
      )
      assert.match(
        await send('XAUTOCLAIM', key('xautoclaim'), 'g', 'c', '0', '0'),
        /^-NOGROUP /,
      )
      assert.strictEqual(
        await send('RPUSH', key('list'), '3', '1', '2'),
        ':3\r\n',
      )
      assert.strictEqual(await send('HSET', key('h_1'), 'f', 'one'), ':1\r\n')
      // BY keys in the list's order, then GET keys in the sorted order.
      assert.strictEqual(
        await send('SORT', key('list'), 'BY', key('w_*'), 'GET', key('g_*')),
        '*3\r\n$-1\r\n$-1\r\n$-1\r\n',
      )
      // No lookup for BY nosort or GET #; `->f` reads the hash key.
      assert.strictEqual(
        await send(
          'SORT',
          key('list'),
          'BY',
          'nosort',
          'GET',
          key('g_*'),
          'GET',
          '#',
          'GET',
          `${key('h_*')}->f`,
        ),
        '*9\r\n$-1\r\n$1\r\n3\r\n$-1\r\n$-1\r\n$1\r\n1\r\n$3\r\none\r\n$-1\r\n$1\r\n2\r\n$-1\r\n',
      )
      // 6.2 looks the patterns up for writing under STORE.
      assert.strictEqual(
        await send('SORT', key('list'), 'BY', key('w_*'), 'STORE', key('dst')),
        ':3\r\n',
      )
      if (hgetex) {
        assert.strictEqual(
          await send('HGETEX', key('hgetex'), 'FIELDS', '1', 'f'),
          '*1\r\n$-1\r\n',
        )
      }
    })
    assert.deepStrictEqual(events, [
      'keymiss bf',
      'keymiss bf-wrap',
      'keymiss xack',
      'keymiss xclaim',
      'keymiss xautoclaim',
      'keymiss w_3',
      'keymiss w_1',
      'keymiss w_2',
      'keymiss g_1',
      'keymiss g_2',
      'keymiss g_3',
      'keymiss g_3',
      'keymiss h_3',
      'keymiss g_1',
      'keymiss g_2',
      'keymiss h_2',
      ...(legacy ? [] : ['keymiss w_3', 'keymiss w_1', 'keymiss w_2']),
      // Redis 8.0's HGETEX looks the hash up for writing.
      ...(hgetex && profile === 'valkey-9.0' ? ['keymiss hgetex'] : []),
    ])
  })

  test('store commands read their sources from 7.0', async () => {
    const events = await capture(legacy ? 'Em' : 'Emn', async key => {
      assert.strictEqual(await send('SADD', key('set'), 'a'), ':1\r\n')
      assert.strictEqual(
        await send('SUNIONSTORE', key('dst'), key('set'), key('missing')),
        ':1\r\n',
      )
      assert.strictEqual(await send('PFCOUNT', key('hll')), ':0\r\n')
      assert.strictEqual(
        await send('PFCOUNT', key('hll1'), key('hll2')),
        ':0\r\n',
      )
      assert.strictEqual(
        await send('BITOP', 'AND', key('bitop'), key('bits')),
        ':0\r\n',
      )
    })
    assert.deepStrictEqual(
      events,
      legacy
        ? ['keymiss hll1', 'keymiss hll2', 'keymiss bits']
        : [
            'new set',
            'keymiss missing',
            'new dst',
            'keymiss hll',
            'keymiss hll1',
            'keymiss hll2',
            'keymiss bits',
          ],
    )
  })

  test('both channels carry keymiss and new, before the command event', async () => {
    const events = await capture(legacy ? 'KEm$' : 'KEmn$', async key => {
      assert.strictEqual(await send('GET', key('k')), '$-1\r\n')
      assert.strictEqual(await send('SET', key('k'), 'v'), '+OK\r\n')
    })
    assert.deepStrictEqual(events, [
      'k: keymiss',
      'keymiss k',
      ...(legacy ? [] : ['k: new', 'new k']),
      'k: set',
      'set k',
    ])
  })
})
