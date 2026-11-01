import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import {
  RawRedisConnection,
  respText,
  type RespWireValue,
} from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * DELIFEQ key value (#645): Valkey 9.0 only (valkey@9.0.0 t_string.c
 * delifeqCommand), an unknown command on every Redis version and on Valkey
 * 8.0. It deletes a string key when its value equals `value` byte for byte
 * (an integer-encoded value compares by its string form), answers 1 / 0,
 * WRONGTYPE for a non-string, and announces the delete as `del`. Checked
 * against valkey-server 9.0.0 / 8.0.0 and redis-server 6.2.14 / 8.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const supported = profile === 'valkey-9.0'
const legacy = profile === 'redis-6.2'

function unknownCommand(name: string, args: string[]): string {
  return legacy
    ? `-ERR unknown command \`${name}\`, with args beginning with: ${args.map(arg => `\`${arg}\`, `).join('')}\r\n`
    : `-ERR unknown command '${name}', with args beginning with: ${args.map(arg => `'${arg}' `).join('')}\r\n`
}

function bulk(text: string): string {
  return `$${Buffer.byteLength(text)}\r\n${text}\r\n`
}

const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const ARITY = "-ERR wrong number of arguments for 'delifeq' command\r\n"

const DELIFEQ_COMMAND_INFO =
  '*1\r\n*10\r\n$7\r\ndelifeq\r\n:3\r\n*2\r\n+write\r\n+fast\r\n:1\r\n:1\r\n:1\r\n' +
  '*3\r\n+@write\r\n+@string\r\n+@fast\r\n*0\r\n' +
  '*1\r\n*6\r\n$5\r\nflags\r\n*2\r\n+RM\r\n+delete\r\n' +
  '$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n*2\r\n$5\r\nindex\r\n:1\r\n' +
  '$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n' +
  '*6\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n'

const DELIFEQ_COMMAND_DOCS =
  '*2\r\n$7\r\ndelifeq\r\n*10\r\n' +
  bulk('summary') +
  bulk('Delete key if value matches string.') +
  bulk('since') +
  bulk('9.0.0') +
  bulk('group') +
  bulk('string') +
  bulk('complexity') +
  bulk('O(1)') +
  bulk('arguments') +
  '*2\r\n' +
  '*8\r\n' +
  bulk('name') +
  bulk('key') +
  bulk('type') +
  bulk('key') +
  bulk('display_text') +
  bulk('key') +
  bulk('key_spec_index') +
  ':0\r\n' +
  '*6\r\n' +
  bulk('name') +
  bulk('value') +
  bulk('type') +
  bulk('string') +
  bulk('display_text') +
  bulk('value')

describe(`DELIFEQ by profile (${profile}, ${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    return connection
  }

  test('unknown before Valkey 9.0', async t => {
    if (supported) {
      t.skip('DELIFEQ exists on Valkey 9.0')
      return
    }
    const conn = await connect()
    const key = `delifeq:${randomKey()}`
    await expectReply(conn, ['SET', key, 'a'], '+OK\r\n')
    await expectReply(
      conn,
      ['DELIFEQ', key, 'a'],
      unknownCommand('DELIFEQ', [key, 'a']),
    )
    await expectReply(conn, ['EXISTS', key], ':1\r\n')
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(
      conn,
      ['DELIFEQ', key, 'a'],
      unknownCommand('DELIFEQ', [key, 'a']),
    )
    await expectReply(
      conn,
      ['EXEC'],
      '-EXECABORT Transaction discarded because of previous errors.\r\n',
    )
    await expectReply(conn, ['COMMAND', 'INFO', 'delifeq'], '*1\r\n$-1\r\n')
    if (!legacy) {
      // Redis 6.2 has no COMMAND DOCS.
      await expectReply(conn, ['COMMAND', 'DOCS', 'delifeq'], '*0\r\n')
    }
    await expectReply(
      conn,
      ['COMMAND', 'GETKEYS', 'DELIFEQ', key, 'a'],
      '-ERR Invalid command specified\r\n',
    )
  })

  test('deletes on a matching value (Valkey 9.0)', async t => {
    if (!supported) {
      t.skip('DELIFEQ is Valkey 9.0 only')
      return
    }
    const conn = await connect()
    const key = `delifeq:${randomKey()}`
    await expectReply(conn, ['SET', key, 'a'], '+OK\r\n')
    await expectReply(conn, ['DELIFEQ', key, 'b'], ':0\r\n')
    await expectReply(conn, ['EXISTS', key], ':1\r\n')
    await expectReply(conn, ['DELIFEQ', key, 'a'], ':1\r\n')
    await expectReply(conn, ['GET', key], '$-1\r\n')
    await expectReply(conn, ['DELIFEQ', key, 'a'], ':0\r\n')

    // An integer-encoded value compares by its string form.
    const int = `delifeq:${randomKey()}:int`
    await expectReply(conn, ['SET', int, '5'], '+OK\r\n')
    await expectReply(conn, ['DELIFEQ', int, '5'], ':1\r\n')
    const float = `delifeq:${randomKey()}:float`
    await expectReply(conn, ['SET', float, '1.0'], '+OK\r\n')
    await expectReply(conn, ['DELIFEQ', float, '1'], ':0\r\n')
    await expectReply(conn, ['DELIFEQ', float, '1.0'], ':1\r\n')
    const empty = `delifeq:${randomKey()}:empty`
    await expectReply(conn, ['SET', empty, ''], '+OK\r\n')
    await expectReply(conn, ['DELIFEQ', empty, ''], ':1\r\n')
  })

  test('WRONGTYPE and arity (Valkey 9.0)', async t => {
    if (!supported) {
      t.skip('DELIFEQ is Valkey 9.0 only')
      return
    }
    const conn = await connect()
    const list = `delifeq:${randomKey()}:list`
    await expectReply(conn, ['LPUSH', list, 'a'], ':1\r\n')
    await expectReply(conn, ['DELIFEQ', list, 'a'], WRONGTYPE)
    await expectReply(conn, ['DELIFEQ', list], ARITY)
    await expectReply(conn, ['DELIFEQ', list, 'a', 'extra'], ARITY)
  })

  test('inside MULTI and from scripts (Valkey 9.0)', async t => {
    if (!supported) {
      t.skip('DELIFEQ is Valkey 9.0 only')
      return
    }
    const conn = await connect()
    const key = `delifeq:${randomKey()}`
    await expectReply(conn, ['SET', key, 'a'], '+OK\r\n')
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['DELIFEQ', key, 'a'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*1\r\n:1\r\n')

    const script = "return redis.call('DELIFEQ', KEYS[1], ARGV[1])"
    await expectReply(conn, ['SET', key, 'a'], '+OK\r\n')
    await expectReply(conn, ['EVAL', script, '1', key, 'a'], ':1\r\n')
    await expectReply(conn, ['EVAL', script, '1', key, 'a'], ':0\r\n')
    const list = `delifeq:${randomKey()}:list`
    await expectReply(conn, ['LPUSH', list, 'a'], ':1\r\n')
    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('DELIFEQ', KEYS[1], ARGV[1])",
        '1',
        list,
        'a',
      ],
      WRONGTYPE,
    )
  })

  test('announces del and touches WATCH only on a delete (Valkey 9.0)', async t => {
    if (!supported) {
      t.skip('DELIFEQ is Valkey 9.0 only')
      return
    }
    const actor = await connect()
    const watcher = await connect()
    const subscriber = await connect()
    const key = `delifeq:${randomKey()}`
    const done = `__keyevent@0__:done-${key}`
    subscriber.write(
      commandFrame(
        'SUBSCRIBE',
        `__keyspace@0__:${key}`,
        '__keyevent@0__:del',
        done,
      ),
    )
    for (let i = 0; i < 3; i++) await subscriber.readFrame()

    await expectReply(
      actor,
      ['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'],
      '+OK\r\n',
    )
    try {
      await expectReply(actor, ['SET', key, 'a'], '+OK\r\n')
      // A miss leaves a WATCH intact.
      await expectReply(watcher, ['WATCH', key], '+OK\r\n')
      await expectReply(actor, ['DELIFEQ', key, 'b'], ':0\r\n')
      await expectReply(watcher, ['MULTI'], '+OK\r\n')
      await expectReply(watcher, ['EXISTS', key], '+QUEUED\r\n')
      await expectReply(watcher, ['EXEC'], '*1\r\n:1\r\n')

      // A delete dirties it.
      await expectReply(watcher, ['WATCH', key], '+OK\r\n')
      await expectReply(actor, ['DELIFEQ', key, 'a'], ':1\r\n')
      await expectReply(watcher, ['MULTI'], '+OK\r\n')
      await expectReply(watcher, ['EXISTS', key], '+QUEUED\r\n')
      await expectReply(watcher, ['EXEC'], '*-1\r\n')
    } finally {
      await expectReply(
        actor,
        ['CONFIG', 'SET', 'notify-keyspace-events', ''],
        '+OK\r\n',
      )
    }
    await expectReply(actor, ['PUBLISH', done, 'x'], ':1\r\n')

    const events: string[] = []
    for (;;) {
      const frame = (await subscriber.readFrame()) as RespWireValue[]
      const channel = respText(frame[1])
      const payload = respText(frame[2])
      if (channel === done) break
      if (channel === '__keyevent@0__:del' && payload !== key) continue
      events.push(`${channel} ${payload}`)
    }
    assert.deepStrictEqual(events, [
      `__keyspace@0__:${key} set`,
      `__keyspace@0__:${key} del`,
      `__keyevent@0__:del ${key}`,
    ])
  })

  test('COMMAND INFO, DOCS and GETKEYS (Valkey 9.0)', async t => {
    if (!supported) {
      t.skip('DELIFEQ is Valkey 9.0 only')
      return
    }
    const conn = await connect()
    await expectReply(
      conn,
      ['COMMAND', 'INFO', 'delifeq'],
      DELIFEQ_COMMAND_INFO,
    )
    await expectReply(
      conn,
      ['COMMAND', 'DOCS', 'delifeq'],
      DELIFEQ_COMMAND_DOCS,
    )
    await expectReply(
      conn,
      ['COMMAND', 'GETKEYS', 'DELIFEQ', 'k', 'a'],
      '*1\r\n$1\r\nk\r\n',
    )
  })
})
