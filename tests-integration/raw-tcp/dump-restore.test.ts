import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'

import { crc64 } from '../../src/state/rdb/crc64'
import { RDB_VERSION } from '../dump-payload-cases'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { expectReply, send } from './helpers'
import { RawRedisConnection } from './raw-connection'

/**
 * DUMP key / RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s]
 * [FREQ f] (#202). The replies are the same on redis 6.2.14, 7.0.15, 7.2.4,
 * 7.4.4, 8.0.0 and valkey 8.0.0 / 9.0.0 (cluster.c restoreCommand); only the
 * payload bytes vary, which compatibility/dump-restore-gates.test.ts covers.
 */
const testRunner = new TestRunner()

const BUSYKEY = '-BUSYKEY Target key name already exists.\r\n'
const SYNTAX = '-ERR syntax error\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const INVALID_TTL = '-ERR Invalid TTL value, must be >= 0\r\n'
const INVALID_IDLETIME = '-ERR Invalid IDLETIME value, must be >= 0\r\n'
const INVALID_FREQ = '-ERR Invalid FREQ value, must be >= 0 and <= 255\r\n'
const PAYLOAD_ERROR = '-ERR DUMP payload version or checksum are wrong\r\n'
const BAD_FORMAT = '-ERR Bad data format\r\n'

function arity(name: string): string {
  return `-ERR wrong number of arguments for '${name}' command\r\n`
}

/** A payload around `body` with the active profile's version and a valid CRC. */
function forge(body: Buffer, version = RDB_VERSION[activeProfile]): Buffer {
  const signed = Buffer.concat([
    body,
    Buffer.from([version & 0xff, version >> 8]),
  ])
  return Buffer.concat([signed, crc64(signed)])
}

function bulkFrame(value: Buffer): Buffer {
  return Buffer.concat([
    Buffer.from(`$${value.length}\r\n`),
    value,
    Buffer.from('\r\n'),
  ])
}

describe(`Raw TCP DUMP / RESTORE (${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) connection.close()
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    return connection
  }

  /** A key holding "v", and its DUMP payload. */
  async function source(
    conn: RawRedisConnection,
  ): Promise<{ key: string; payload: Buffer }> {
    const key = `dr:${randomKey()}:src`
    await expectReply(conn, ['SET', key, 'v'], '+OK\r\n')
    conn.write(commandFrame('DUMP', key))
    const frame = await conn.readRawFrame()
    const header = frame.indexOf('\r\n')
    const length = Number(frame.subarray(1, header).toString())
    return { key, payload: frame.subarray(header + 2, header + 2 + length) }
  }

  const fresh = () => `dr:${randomKey()}:dst`

  test('DUMP of a missing key is a null bulk; arity errors', async () => {
    const conn = await connect()
    await expectReply(conn, ['DUMP', fresh()], '$-1\r\n')
    await expectReply(conn, ['DUMP'], arity('dump'))
    await expectReply(conn, ['DUMP', 'a', 'b'], arity('dump'))
    await expectReply(conn, ['RESTORE', fresh(), '0'], arity('restore'))
  })

  test('RESTORE recreates the value; ttl 0 keeps it persistent', async () => {
    const conn = await connect()
    const { payload } = await source(conn)
    const key = fresh()
    await expectReply(conn, ['RESTORE', key, '0', payload], '+OK\r\n')
    await expectReply(conn, ['GET', key], '$1\r\nv\r\n')
    await expectReply(conn, ['TTL', key], ':-1\r\n')
    await expectReply(conn, ['DUMP', key], bulkFrame(payload))
  })

  test('an existing key answers BUSYKEY before the ttl and payload are read', async () => {
    const conn = await connect()
    const { key, payload } = await source(conn)
    await expectReply(conn, ['RESTORE', key, '0', payload], BUSYKEY)
    await expectReply(conn, ['RESTORE', key, '-1', payload], BUSYKEY)
    await expectReply(conn, ['RESTORE', key, 'abc', 'junk'], BUSYKEY)
    // Options are parsed first.
    await expectReply(conn, ['RESTORE', key, '0', payload, 'BOGUS'], SYNTAX)
    await expectReply(
      conn,
      ['RESTORE', key, '0', payload, 'FREQ', '300'],
      INVALID_FREQ,
    )
  })

  test('REPLACE overwrites an existing key; option names ignore case', async () => {
    const conn = await connect()
    const { payload } = await source(conn)
    const key = fresh()
    await expectReply(conn, ['RPUSH', key, 'x'], ':1\r\n')
    await expectReply(
      conn,
      ['RESTORE', key, '0', payload, 'replace'],
      '+OK\r\n',
    )
    await expectReply(conn, ['GET', key], '$1\r\nv\r\n')
  })

  test('ttl must be a non-negative integer', async () => {
    const conn = await connect()
    const { payload } = await source(conn)
    await expectReply(conn, ['RESTORE', fresh(), '-1', payload], INVALID_TTL)
    await expectReply(
      conn,
      ['RESTORE', fresh(), '-1', payload, 'ABSTTL'],
      INVALID_TTL,
    )
    await expectReply(conn, ['RESTORE', fresh(), 'abc', payload], NOT_INTEGER)
    await expectReply(conn, ['RESTORE', fresh(), '1.5', payload], NOT_INTEGER)
  })

  test('a relative or ABSTTL deadline sets the expiry', async () => {
    const conn = await connect()
    const { payload } = await source(conn)
    const relative = fresh()
    await expectReply(conn, ['RESTORE', relative, '100000', payload], '+OK\r\n')
    conn.write(commandFrame('PTTL', relative))
    const pttl = Number((await conn.readFrame()) as number)
    assert.ok(pttl > 90000 && pttl <= 100000, String(pttl))

    const absolute = fresh()
    const at = String(Date.now() + 100000)
    await expectReply(
      conn,
      ['RESTORE', absolute, at, payload, 'ABSTTL'],
      '+OK\r\n',
    )
    conn.write(commandFrame('PTTL', absolute))
    const left = Number((await conn.readFrame()) as number)
    assert.ok(left > 80000 && left <= 110000, String(left))
  })

  test('a deadline already past creates nothing; with REPLACE it deletes the key', async () => {
    const conn = await connect()
    const { payload } = await source(conn)
    const key = fresh()
    await expectReply(conn, ['RESTORE', key, '1', payload, 'ABSTTL'], '+OK\r\n')
    await expectReply(conn, ['EXISTS', key], ':0\r\n')
    await expectReply(conn, ['SET', key, 'old'], '+OK\r\n')
    await expectReply(
      conn,
      ['RESTORE', key, '1', payload, 'ABSTTL', 'REPLACE'],
      '+OK\r\n',
    )
    await expectReply(conn, ['EXISTS', key], ':0\r\n')
  })

  test('a relative ttl whose deadline overflows int64 counts as already expired', async () => {
    const conn = await connect()
    const { payload } = await source(conn)
    const key = fresh()
    const max = '9223372036854775807'
    await expectReply(conn, ['RESTORE', key, max, payload], '+OK\r\n')
    await expectReply(conn, ['EXISTS', key], ':0\r\n')
    await expectReply(conn, ['SET', key, 'old'], '+OK\r\n')
    await expectReply(
      conn,
      ['RESTORE', key, max, payload, 'REPLACE'],
      '+OK\r\n',
    )
    await expectReply(conn, ['EXISTS', key], ':0\r\n')
    // The same ttl as an absolute deadline lies far in the future.
    await expectReply(conn, ['RESTORE', key, max, payload, 'ABSTTL'], '+OK\r\n')
    await expectReply(conn, ['EXISTS', key], ':1\r\n')
  })

  test('IDLETIME and FREQ: ranges, exclusivity and argument consumption', async () => {
    const conn = await connect()
    const { payload } = await source(conn)
    await expectReply(
      conn,
      ['RESTORE', fresh(), '0', payload, 'IDLETIME', '10'],
      '+OK\r\n',
    )
    await expectReply(
      conn,
      ['RESTORE', fresh(), '0', payload, 'FREQ', '5'],
      '+OK\r\n',
    )
    const cases: Array<[string[], string]> = [
      [['IDLETIME', '-1'], INVALID_IDLETIME],
      [['IDLETIME', 'x'], NOT_INTEGER],
      [['IDLETIME', 'REPLACE'], NOT_INTEGER],
      [['IDLETIME'], SYNTAX],
      [['FREQ', '256'], INVALID_FREQ],
      [['FREQ', '-1'], INVALID_FREQ],
      [['FREQ'], SYNTAX],
      [['IDLETIME', '1', 'FREQ', '1'], SYNTAX],
      [['FREQ', '1', 'IDLETIME', '1'], SYNTAX],
    ]
    for (const [options, reply] of cases) {
      await expectReply(
        conn,
        ['RESTORE', fresh(), '0', payload, ...options],
        reply,
      )
    }
  })

  test('payloads with a wrong version, checksum or format', async () => {
    const conn = await connect()
    const { payload } = await source(conn)
    const corrupt = Buffer.from(payload)
    corrupt[corrupt.length - 1] ^= 0xff
    await expectReply(conn, ['RESTORE', fresh(), '0', corrupt], PAYLOAD_ERROR)
    await expectReply(conn, ['RESTORE', fresh(), '0', ''], PAYLOAD_ERROR)
    await expectReply(conn, ['RESTORE', fresh(), '0', 'short'], PAYLOAD_ERROR)
    await expectReply(
      conn,
      ['RESTORE', fresh(), '0', forge(Buffer.from('000176', 'hex'), 0xffff)],
      PAYLOAD_ERROR,
    )
    await expectReply(
      conn,
      ['RESTORE', fresh(), '0', forge(Buffer.from('7e', 'hex'))],
      BAD_FORMAT,
    )
  })

  test('MULTI queues DUMP and RESTORE', async () => {
    const conn = await connect()
    const { key, payload } = await source(conn)
    const target = fresh()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['DUMP', key], '+QUEUED\r\n')
    await expectReply(conn, ['RESTORE', target, '0', payload], '+QUEUED\r\n')
    await expectReply(conn, ['RESTORE', target, '0', payload], '+QUEUED\r\n')
    await expectReply(
      conn,
      ['EXEC'],
      Buffer.concat([
        Buffer.from('*3\r\n'),
        bulkFrame(payload),
        Buffer.from('+OK\r\n'),
        Buffer.from(BUSYKEY),
      ]),
    )
  })

  test('scripts call DUMP and RESTORE', async () => {
    const conn = await connect()
    const { key, payload } = await source(conn)
    await expectReply(
      conn,
      ['EVAL', "return redis.call('DUMP', KEYS[1])", '1', key],
      bulkFrame(payload),
    )
    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.call('RESTORE', KEYS[1], 0, ARGV[1])",
        '1',
        fresh(),
        payload,
      ],
      '+OK\r\n',
    )
    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('RESTORE', KEYS[1], 0, ARGV[1])",
        '1',
        key,
        payload,
      ],
      BUSYKEY,
    )
  })

  test('keyspace events: restore, or del when REPLACE meets a past deadline', async () => {
    const conn = await connect()
    const sub = await connect()
    const { payload } = await source(conn)
    conn.write(commandFrame('CONFIG', 'GET', 'notify-keyspace-events'))
    const previous = (await conn.readFrame()) as Buffer[]
    await expectReply(
      conn,
      ['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'],
      '+OK\r\n',
    )
    try {
      const key = fresh()
      const channel = `__keyspace@0__:${key}`
      sub.write(commandFrame('SUBSCRIBE', channel))
      await sub.readFrame()
      await expectReply(conn, ['RESTORE', key, '0', payload], '+OK\r\n')
      await expectReply(
        conn,
        ['RESTORE', key, '1', payload, 'ABSTTL', 'REPLACE'],
        '+OK\r\n',
      )
      // Already expired with no existing key: nothing at all.
      await expectReply(
        conn,
        ['RESTORE', key, '1', payload, 'ABSTTL'],
        '+OK\r\n',
      )
      await expectReply(conn, ['SET', key, 'x'], '+OK\r\n')
      const events: string[] = []
      for (let i = 0; i < 3; i++) {
        const message = (await sub.readFrame()) as Buffer[]
        events.push(message[2].toString())
      }
      assert.deepStrictEqual(events, ['restore', 'del', 'set'])
    } finally {
      await send(conn, [
        'CONFIG',
        'SET',
        'notify-keyspace-events',
        previous[1].toString(),
      ])
    }
  })

  test('WATCH: a RESTORE dirties the key, a refused one does not', async () => {
    const conn = await connect()
    const other = await connect()
    const { key, payload } = await source(conn)
    const target = fresh()

    await expectReply(conn, ['WATCH', target], '+OK\r\n')
    await expectReply(other, ['RESTORE', target, '0', payload], '+OK\r\n')
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['PING'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*-1\r\n')

    await expectReply(conn, ['WATCH', key], '+OK\r\n')
    await expectReply(other, ['RESTORE', key, '0', payload], BUSYKEY)
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['PING'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*1\r\n+PONG\r\n')
  })
})
