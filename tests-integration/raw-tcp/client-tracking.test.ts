import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import {
  TrackingClient,
  invalidate,
  invalidateAll,
  invalidateAllMessage,
  invalidateMessage,
} from './tracking-client'

/**
 * CLIENT TRACKING (#252): replies, errors, CLIENT LIST fields, and the
 * invalidations of default, OPTIN, OPTOUT, NOLOOP, BCAST and REDIRECT mode.
 * Identical on every profile (redis@7.2.4 tracking.c; valkey 8/9 the same):
 * checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

// A database no other suite selects: the FLUSHDB test empties it.
const DB = '14'

const err = (message: string) => `-ERR ${message}\r\n`

describe(`CLIENT TRACKING (${testRunner.getBackendName()}, ${profile})`, () => {
  let port: number
  const opened: TrackingClient[] = []
  const run = randomKey()
  const key = (name: string) => `${run}:${name}`

  async function open(resp3: boolean): Promise<TrackingClient> {
    const client = await TrackingClient.open(port, resp3, DB)
    opened.push(client)
    return client
  }

  /** A RESP3 connection with CLIENT TRACKING ON `options`. */
  async function tracker(...options: string[]): Promise<TrackingClient> {
    const client = await open(true)
    assert.strictEqual(
      await client.call('CLIENT', 'TRACKING', 'ON', ...options),
      '+OK\r\n',
    )
    return client
  }

  let writer: TrackingClient

  before(async () => {
    port = await testRunner.setupRawStandalone()
    writer = await open(false)
  })

  after(async () => {
    for (const client of opened) {
      client.close()
    }
    await testRunner.cleanup()
  })

  test('option errors, in parse-then-check order', async () => {
    const client = await open(false)
    const tracking = (...args: string[]) =>
      client.call('CLIENT', 'TRACKING', ...args)
    assert.strictEqual(await tracking('FOO'), err('syntax error'))
    assert.strictEqual(await tracking('ON', 'FOO'), err('syntax error'))
    assert.strictEqual(await tracking('ON', 'REDIRECT'), err('syntax error'))
    assert.strictEqual(await tracking('ON', 'PREFIX'), err('syntax error'))
    assert.strictEqual(
      await tracking('ON', 'PREFIX', 'a'),
      err('PREFIX option requires BCAST mode to be enabled'),
    )
    assert.strictEqual(
      await tracking('ON', 'REDIRECT', '0'),
      err('The client ID you want redirect to does not exist'),
    )
    assert.strictEqual(
      await tracking('ON', 'REDIRECT', 'abc'),
      err('value is not an integer or out of range'),
    )
    const own = await client.id()
    assert.strictEqual(
      await tracking('ON', 'REDIRECT', own, 'REDIRECT', own),
      err('A client can only redirect to a single other client'),
    )
    assert.strictEqual(
      await tracking('ON', 'BCAST', 'OPTIN'),
      err('OPTIN and OPTOUT are not compatible with BCAST'),
    )
    assert.strictEqual(
      await tracking('ON', 'OPTIN', 'OPTOUT'),
      err("You can't specify both OPTIN mode and OPTOUT mode"),
    )
    assert.strictEqual(
      await tracking('ON', 'BCAST', 'PREFIX', 'ab', 'PREFIX', 'abc'),
      err(
        "Prefix 'ab' overlaps with another provided prefix 'abc'. Prefixes for a single client must not overlap.",
      ),
    )
    assert.strictEqual(
      await client.call('CLIENT', 'CACHING', 'YES'),
      err(
        'CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled',
      ),
    )
    assert.strictEqual(await tracking('on', 'optin'), '+OK\r\n')
    assert.strictEqual(
      await tracking('ON', 'BCAST'),
      err(
        "You can't switch BCAST mode on/off before disabling tracking for this client, and then re-enabling it with a different mode.",
      ),
    )
    assert.strictEqual(
      await tracking('ON', 'OPTOUT'),
      err(
        "You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, and then re-enabling it with a different mode.",
      ),
    )
    assert.strictEqual(
      await client.call('CLIENT', 'CACHING', 'NO'),
      err(
        'CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.',
      ),
    )
    assert.strictEqual(
      await client.call('CLIENT', 'CACHING', 'maybe'),
      err('syntax error'),
    )
    assert.strictEqual(await client.call('CLIENT', 'CACHING', 'yes'), '+OK\r\n')
    assert.strictEqual(await tracking('OFF'), '+OK\r\n')
    assert.strictEqual(await tracking('ON', 'BCAST', 'PREFIX', 'ab'), '+OK\r\n')
    assert.strictEqual(
      await tracking('ON', 'BCAST', 'PREFIX', 'abc'),
      err(
        "Prefix 'abc' overlaps with an existing prefix 'ab'. Prefixes for a single client must not overlap.",
      ),
    )
    assert.strictEqual(await tracking('OFF'), '+OK\r\n')
    // The existing prefixes are checked in bytewise order.
    assert.strictEqual(await tracking('ON', 'BCAST', 'PREFIX', 'ac'), '+OK\r\n')
    assert.strictEqual(await tracking('ON', 'BCAST', 'PREFIX', 'ab'), '+OK\r\n')
    assert.strictEqual(
      await tracking('ON', 'BCAST', 'PREFIX', 'a'),
      err(
        "Prefix 'a' overlaps with an existing prefix 'ab'. Prefixes for a single client must not overlap.",
      ),
    )
    assert.strictEqual(await tracking('OFF'), '+OK\r\n')
  })

  test('TRACKINGINFO, GETREDIR and the CLIENT INFO fields', async () => {
    const client = await open(false)
    const info = () => client.call('CLIENT', 'TRACKINGINFO')
    const infoReply = (flags: string[], redirect: string, prefixes: string[]) =>
      `*6\r\n$5\r\nflags\r\n*${flags.length}\r\n${flags.map(f => `$${f.length}\r\n${f}\r\n`).join('')}` +
      `$8\r\nredirect\r\n:${redirect}\r\n$8\r\nprefixes\r\n*${prefixes.length}\r\n` +
      prefixes.map(p => `$${p.length}\r\n${p}\r\n`).join('')
    const clientInfo = async () => {
      const line = await client.call('CLIENT', 'INFO')
      return {
        flags: /flags=(\S+)/.exec(line)?.[1],
        redir: /redir=(\S+)/.exec(line)?.[1],
      }
    }

    assert.strictEqual(await info(), infoReply(['off'], '-1', []))
    assert.strictEqual(await client.call('CLIENT', 'GETREDIR'), ':-1\r\n')
    assert.deepStrictEqual(await clientInfo(), { flags: 'N', redir: '-1' })

    await client.call('CLIENT', 'TRACKING', 'ON', 'OPTIN')
    await client.call('CLIENT', 'CACHING', 'YES')
    assert.strictEqual(
      await info(),
      infoReply(['on', 'optin', 'caching-yes'], '0', []),
    )
    assert.strictEqual(await client.call('CLIENT', 'GETREDIR'), ':0\r\n')
    assert.deepStrictEqual(await clientInfo(), { flags: 't', redir: '0' })
    await client.call('CLIENT', 'TRACKING', 'OFF')

    const other = await open(false)
    const otherId = await other.id()
    await client.call(
      'CLIENT',
      'TRACKING',
      'ON',
      'BCAST',
      'PREFIX',
      'x',
      'PREFIX',
      'ab',
      'NOLOOP',
      'REDIRECT',
      otherId,
    )
    assert.strictEqual(
      await info(),
      infoReply(['on', 'bcast', 'noloop'], otherId, ['ab', 'x']),
    )
    assert.strictEqual(
      await client.call('CLIENT', 'GETREDIR'),
      `:${otherId}\r\n`,
    )
    assert.deepStrictEqual(await clientInfo(), { flags: 'tB', redir: otherId })
    await client.call('CLIENT', 'TRACKING', 'OFF')

    await client.call('CLIENT', 'TRACKING', 'ON', 'BCAST')
    assert.strictEqual(await info(), infoReply(['on', 'bcast'], '0', ['']))
    assert.strictEqual(await client.call('RESET'), '+RESET\r\n')
    assert.strictEqual(await info(), infoReply(['off'], '-1', []))
  })

  test('default mode: one invalidation per read key, then forgotten', async () => {
    const client = await tracker()
    const [a, b, missing] = [key('d:a'), key('d:b'), key('d:missing')]
    await writer.call('MSET', a, '1', b, '2')
    await client.call('MGET', a, b)
    await client.call('GET', missing)
    await client.call('GET', a)

    await writer.call('SET', a, '3')
    await writer.call('DEL', missing)
    assert.deepStrictEqual(await client.pending(), [invalidate(a)])

    await writer.call('SET', a, '4')
    await writer.call('SET', missing, '1')
    await writer.call('LPUSH', key('d:never-read'), 'x')
    assert.deepStrictEqual(await client.pending(), [invalidate(missing)])

    // A write remembers nothing, not even its own key.
    const written = key('d:written')
    await client.call('SET', written, '1')
    await writer.call('SET', written, '2')
    assert.deepStrictEqual(await client.pending(), [])

    await writer.call('DEL', b)
    assert.deepStrictEqual(await client.pending(), [invalidate(b)])
  })

  test('own writes: the invalidation follows the reply', async () => {
    const client = await tracker()
    const [k, m, e] = [key('own:k'), key('own:m'), key('own:e')]
    await client.call('GET', k)
    assert.deepStrictEqual(await client.callFrames(2, 'SET', k, 'v'), [
      '+OK\r\n',
      invalidate(k),
    ])

    await client.call('GET', m)
    assert.strictEqual(await client.call('MULTI'), '+OK\r\n')
    assert.strictEqual(await client.call('INCR', m), '+QUEUED\r\n')
    assert.deepStrictEqual(await client.callFrames(2, 'EXEC'), [
      '*1\r\n:1\r\n',
      invalidate(m),
    ])

    await client.call('GET', e)
    assert.deepStrictEqual(
      await client.callFrames(
        2,
        'EVAL',
        "redis.call('set', KEYS[1], 'x') return 1",
        '1',
        e,
      ),
      [':1\r\n', invalidate(e)],
    )
    assert.deepStrictEqual(await client.pending(), [])
  })

  test('reads inside MULTI/EXEC and scripts are remembered', async () => {
    const client = await tracker()
    const [inMulti, inScript] = [key('rm:multi'), key('rm:script')]
    await client.call('MULTI')
    await client.call('GET', inMulti)
    await client.call('EXEC')
    await client.call(
      'EVAL',
      "return redis.call('get', KEYS[1])",
      '1',
      inScript,
    )
    await writer.call('MSET', inMulti, '1', inScript, '1')
    assert.deepStrictEqual(await client.pending(), [
      invalidate(inMulti),
      invalidate(inScript),
    ])
  })

  test('expiry invalidates', async () => {
    const client = await tracker()
    const volatile = key('exp')
    await writer.call('SET', volatile, 'v', 'PX', '50')
    await client.call('GET', volatile)
    await delay(150)
    assert.strictEqual(await writer.call('EXISTS', volatile), ':0\r\n')
    assert.deepStrictEqual(await client.pending(), [invalidate(volatile)])
  })

  test('NOLOOP skips the client’s own writes only', async () => {
    const client = await tracker('NOLOOP')
    const [own, theirs] = [key('nl:own'), key('nl:theirs')]
    await client.call('MGET', own, theirs)
    assert.strictEqual(await client.call('SET', own, '1'), '+OK\r\n')
    await writer.call('SET', theirs, '1')
    assert.deepStrictEqual(await client.pending(), [invalidate(theirs)])
  })

  test('OPTIN and OPTOUT follow CLIENT CACHING', async () => {
    const optin = await tracker('OPTIN')
    const [skipped, cached, inMulti] = [
      key('in:skipped'),
      key('in:cached'),
      key('in:multi'),
    ]
    await optin.call('GET', skipped)
    await optin.call('CLIENT', 'CACHING', 'YES')
    await optin.call('GET', cached)
    await optin.call('CLIENT', 'CACHING', 'YES')
    await optin.call('MULTI')
    await optin.call('GET', inMulti)
    await optin.call('EXEC')
    assert.match(await optin.call('CLIENT', 'TRACKINGINFO'), /optin\r\n\$8/)
    await writer.call('MSET', skipped, '1', cached, '1', inMulti, '1')
    assert.deepStrictEqual(await optin.pending(), [
      invalidate(cached),
      invalidate(inMulti),
    ])

    const optout = await tracker('OPTOUT')
    const [notCached, tracked] = [key('out:skipped'), key('out:tracked')]
    await optout.call('CLIENT', 'CACHING', 'NO')
    await optout.call('GET', notCached)
    await optout.call('GET', tracked)
    await writer.call('MSET', notCached, '1', tracked, '1')
    assert.deepStrictEqual(await optout.pending(), [invalidate(tracked)])
  })

  test('BCAST: one sorted push per prefix', async () => {
    const [p1, p2] = [key('bc1:'), key('bc2:')]
    const client = await tracker('BCAST', 'PREFIX', p2, 'PREFIX', p1)
    await writer.call(
      'MSET',
      `${p2}z`,
      '1',
      `${p1}b`,
      '1',
      `${p1}a`,
      '1',
      key('elsewhere'),
      '1',
    )
    await delay(20)
    assert.deepStrictEqual(await client.pending(), [
      invalidate(`${p1}a`, `${p1}b`),
      invalidate(`${p2}z`),
    ])

    const quiet = await tracker('BCAST', 'NOLOOP', 'PREFIX', key('nlb:'))
    assert.strictEqual(await quiet.call('SET', key('nlb:own'), '1'), '+OK\r\n')
    await delay(20)
    assert.deepStrictEqual(await quiet.pending(), [])
    await writer.call('SET', key('nlb:theirs'), '1')
    await delay(20)
    assert.deepStrictEqual(await quiet.pending(), [
      invalidate(key('nlb:theirs')),
    ])
    // A script writes on its own client, so NOLOOP does not filter it.
    assert.strictEqual(
      await quiet.call(
        'EVAL',
        "redis.call('set', KEYS[1], '1'); return 1",
        '1',
        key('nlb:script'),
      ),
      ':1\r\n',
    )
    await delay(20)
    assert.deepStrictEqual(await quiet.pending(), [
      invalidate(key('nlb:script')),
    ])
    // A deadline the client gives in the past deletes on its own behalf
    // (an `expired` event on valkey 8.1+, a `del` elsewhere).
    assert.strictEqual(
      await quiet.call('EXPIRE', key('nlb:own'), '0'),
      ':1\r\n',
    )
    await delay(20)
    assert.deepStrictEqual(await quiet.pending(), [])
  })

  test('FLUSHDB sends every tracking client a null', async () => {
    const client = await tracker()
    await client.call('GET', key('fl'))
    assert.strictEqual(await writer.call('FLUSHDB'), '+OK\r\n')
    const frames = await client.pending(true)
    assert.ok(frames.length >= 1, 'a flush invalidation')
    assert.ok(frames.every(frame => frame === invalidateAll))
    // The flush forgot the key.
    await writer.call('SET', key('fl'), '1')
    assert.deepStrictEqual(await client.pending(), [])
  })

  test('REDIRECT: RESP3 target gets the push, RESP2 subscriber the message', async () => {
    const target3 = await open(true)
    const tracker2 = await open(false)
    const k3 = key('r3')
    await tracker2.call(
      'CLIENT',
      'TRACKING',
      'ON',
      'REDIRECT',
      await target3.id(),
    )
    await tracker2.call('GET', k3)
    await writer.call('SET', k3, '1')
    assert.deepStrictEqual(await target3.pending(), [invalidate(k3)])
    assert.deepStrictEqual(await tracker2.pending(), [])

    const target2 = await open(false)
    const target2Id = await target2.id()
    target2.write('SUBSCRIBE', key('chan'))
    await target2.next()
    const subscriber = target2.asSubscriber()
    const tracker2b = await open(false)
    const k2 = key('r2')
    await tracker2b.call('CLIENT', 'TRACKING', 'ON', 'REDIRECT', target2Id)
    await tracker2b.call('GET', k2)
    await writer.call('SET', k2, '1')
    assert.deepStrictEqual(await subscriber.pending(), [invalidateMessage(k2)])

    // A flush's null is spelled in the tracker's protocol (RESP2: $-1).
    await writer.call('FLUSHDB')
    const flushes = await subscriber.pending(true)
    assert.ok(flushes.length >= 1)
    assert.ok(flushes.every(frame => frame === invalidateAllMessage))
  })

  test('a vanished REDIRECT target breaks the redirect', async () => {
    const target = await open(true)
    const targetId = await target.id()
    const client = await tracker('REDIRECT', targetId)
    const k = key('broken')
    await client.call('GET', k)
    target.close()
    for (let attempt = 0; attempt < 50; attempt++) {
      const list = await writer.call('CLIENT', 'LIST', 'ID', targetId)
      if (!list.includes(`id=${targetId} `)) break
      await delay(20)
    }
    await writer.call('SET', k, '1')
    const broken = `>2\r\n$21\r\ntracking-redir-broken\r\n:${targetId}\r\n`
    assert.deepStrictEqual(await client.pending(), [broken])
    assert.match(await client.call('CLIENT', 'TRACKINGINFO'), /broken_redirect/)
    assert.match(await client.call('CLIENT', 'INFO'), / flags=tR /)
    assert.strictEqual(
      await client.call('CLIENT', 'GETREDIR'),
      `:${targetId}\r\n`,
    )
  })
})
