import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'

/**
 * Keyspace notifications for the connection that runs the command (#587): a
 * RESP3 connection subscribed to its own `__keyspace@` / `__keyevent@`
 * channels. Redis 7.2+ and valkey 8.0+ hold every push for the calling
 * connection until the command's reply is out (`server.pending_push_messages`,
 * joined after the outermost call), so the reply comes first, then the pushes
 * in the order they were made, for a script's inner calls too.
 *
 * Redis 6.2 and 7.0 write each push as it is made, so whether it lands before
 * or after the reply depends on where the command notifies (SET / GET / DEL /
 * RENAME / EVAL before, HSET / LPUSH / EXPIRE after). The mock does not model
 * that per-command order (a known gap): on those profiles only the frames are
 * checked, not their order.
 *
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const replyFirst = !['redis-6.2', 'redis-7.0'].includes(profile)

// A database no other suite selects, so no other test's events reach the
// `__keyevent@` pattern.
const DB = '11'

const bulk = (value: string) => `$${Buffer.byteLength(value)}\r\n${value}\r\n`

describe(`keyspace pushes vs the command reply (${testRunner.getBackendName()}, ${profile})`, () => {
  let conn: RawRedisConnection
  const run = randomKey()
  const keyPattern = `__keyspace@${DB}__:${run}:*`
  const eventPattern = `__keyevent@${DB}__:*`

  const keyspace = (key: string, event: string) =>
    `>4\r\n${bulk('pmessage')}${bulk(keyPattern)}${bulk(`__keyspace@${DB}__:${key}`)}${bulk(event)}`
  const keyevent = (key: string, event: string) =>
    `>4\r\n${bulk('pmessage')}${bulk(eventPattern)}${bulk(`__keyevent@${DB}__:${event}`)}${bulk(key)}`
  const both = (key: string, event: string) => [
    keyspace(key, event),
    keyevent(key, event),
  ]

  async function send(...args: string[]): Promise<string> {
    conn.write(commandFrame(...args))
    return (await conn.readRawFrame()).toString('latin1')
  }

  /** Send one command; expect its reply, then `pushes` in emit order. */
  async function expectReplyThenPushes(
    args: string[],
    reply: string,
    pushes: string[],
  ): Promise<void> {
    conn.write(commandFrame(...args))
    const frames: string[] = []
    while (frames.length < pushes.length + 1) {
      frames.push((await conn.readRawFrame()).toString('latin1'))
    }
    const expected = [reply, ...pushes]
    if (replyFirst) {
      assert.deepStrictEqual(frames, expected)
    } else {
      assert.deepStrictEqual([...frames].sort(), [...expected].sort())
    }
  }

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
    conn.write(commandFrame('HELLO', '3'))
    await conn.readFrame()
    assert.strictEqual(await send('SELECT', DB), '+OK\r\n')
    assert.strictEqual(
      await send('CONFIG', 'SET', 'notify-keyspace-events', 'KEAm'),
      '+OK\r\n',
    )
    conn.write(commandFrame('PSUBSCRIBE', keyPattern, eventPattern))
    await conn.readFrame()
    await conn.readFrame()
  })

  after(async () => {
    try {
      conn.write(commandFrame('PUNSUBSCRIBE'))
      await conn.readFrame()
      await conn.readFrame()
      await send('CONFIG', 'SET', 'notify-keyspace-events', '')
    } finally {
      conn.close()
      await testRunner.cleanup()
    }
  })

  test('SET: reply, then the keyspace and keyevent pushes', async () => {
    const key = `${run}:set`
    await expectReplyThenPushes(['SET', key, 'v'], '+OK\r\n', both(key, 'set'))
  })

  test('GET of a missing key: reply, then keymiss', async () => {
    const key = `${run}:miss`
    await expectReplyThenPushes(['GET', key], '_\r\n', both(key, 'keymiss'))
  })

  test('HSET and LPUSH: reply, then the pushes', async () => {
    const hash = `${run}:hash`
    await expectReplyThenPushes(
      ['HSET', hash, 'f', 'v'],
      ':1\r\n',
      both(hash, 'hset'),
    )
    const list = `${run}:list`
    await expectReplyThenPushes(
      ['LPUSH', list, 'a'],
      ':1\r\n',
      both(list, 'lpush'),
    )
  })

  test('RENAME: reply, then rename_from and rename_to', async () => {
    const from = `${run}:from`
    const to = `${run}:to`
    await expectReplyThenPushes(
      ['SET', from, 'v'],
      '+OK\r\n',
      both(from, 'set'),
    )
    await expectReplyThenPushes(['RENAME', from, to], '+OK\r\n', [
      ...both(from, 'rename_from'),
      ...both(to, 'rename_to'),
    ])
  })

  test('SET ... GET of a missing key: reply, then keymiss and set', async () => {
    const key = `${run}:setget`
    await expectReplyThenPushes(['SET', key, 'v', 'GET'], '_\r\n', [
      ...both(key, 'keymiss'),
      ...both(key, 'set'),
    ])
  })

  test("EVAL: the script's reply, then its inner calls' pushes", async () => {
    const key = `${run}:eval`
    await expectReplyThenPushes(
      ['EVAL', "return redis.call('SET', KEYS[1], 'v')", '1', key],
      '+OK\r\n',
      both(key, 'set'),
    )
  })
})
