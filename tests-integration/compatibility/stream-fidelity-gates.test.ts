import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import {
  RawRedisConnection,
  respNumber,
  respText,
  type RespWireValue,
} from '../raw-tcp/raw-connection'
import { expectReply, send } from '../raw-tcp/helpers'

/**
 * Stream reply fidelity by version (#528, #495 items 1-2, #519 XREAD items):
 * the empty XREAD / XREADGROUP reply, `XREAD ... +`, listing order, XINFO
 * STREAM FULL / XPENDING shapes, the consumer-group lag algorithm, int64
 * counters and the implicit-consumer event.
 *
 * Every expectation follows `REDIS_COMPAT`, so the file also runs against a
 * real server of the matching version (`TEST_BACKEND=real
 * REDIS_STANDALONE_PORT=<port>`). Checked against redis-server 6.2.14,
 * 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0 / 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile

/** 7.0+ / every Valkey: ENTRIESREAD, `entries-read` / `lag`. */
const groupLag = profile !== 'redis-6.2'
/** 7.2+ / every Valkey: a consumer's `active-time`. */
const activeTime = profile !== 'redis-6.2' && profile !== 'redis-7.0'
/** Redis 7.4+ / Valkey 8.0+: `XREAD ... +`. */
const plusId = ['redis-7.4', 'redis-8.0', 'valkey-8.0', 'valkey-9.0'].includes(
  profile,
)
/** Redis 7.4+ / Valkey 9.0+: the stricter estimate and XREADGROUP counter. */
const tombstoneCheck = ['redis-7.4', 'redis-8.0', 'valkey-9.0'].includes(
  profile,
)
/** Redis 7.4+: lag 0 for an emptied stream, `length` for a group behind it. */
const lagShortcuts = ['redis-7.4', 'redis-8.0'].includes(profile)
/** 7.0+: implicit consumers publish `xgroup-createconsumer`. */
const implicitEvent = profile !== 'redis-6.2'

const OK = '+OK\r\n'
const NULL_ARRAY = '*-1\r\n'
const NIL = '$-1\r\n'
const INVALID_ID =
  '-ERR Invalid stream ID specified as stream command argument\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const INT64_MAX = '9223372036854775807'

function bulk(value: string): string {
  return `$${Buffer.byteLength(value)}\r\n${value}\r\n`
}

function int(value: number | bigint | string): string {
  return `:${value}\r\n`
}

function meaningless(id: string): string {
  return `-ERR The ${id} ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The ${id} ID would just return an empty result set.\r\n`
}

describe(`stream reply fidelity by version (${testRunner.getBackendName()}, ${profile})`, () => {
  let conn: RawRedisConnection
  let port: number
  const RUN = randomKey()
  let counter = 0
  const used: string[] = []

  function key(label: string): string {
    const name = `streamfidelity:{${RUN}}:${label}:${counter++}`
    used.push(name)
    return name
  }

  async function command(...args: string[]): Promise<RespWireValue> {
    conn.write(commandFrame(...args))
    return conn.readFrame()
  }

  /** A stream with entries 1-1 .. n-1 and group `g` at 0. */
  async function streamWithGroup(label: string, n: number): Promise<string> {
    const k = key(label)
    for (let i = 1; i <= n; i++)
      await send(conn, ['XADD', k, `${i}-1`, 'f', 'v'])
    await expectReply(conn, ['XGROUP', 'CREATE', k, 'g', '0'], OK)
    return k
  }

  async function read(k: string, consumer: string, count = 1): Promise<void> {
    await send(conn, [
      'XREADGROUP',
      'GROUP',
      'g',
      consumer,
      'COUNT',
      String(count),
      'STREAMS',
      k,
      '>',
    ])
  }

  /** The flat RESP2 field/value array as a lookup. */
  function fields(reply: RespWireValue): Map<string, RespWireValue> {
    assert.ok(Array.isArray(reply))
    const map = new Map<string, RespWireValue>()
    for (let i = 0; i < reply.length; i += 2) {
      map.set(respText(reply[i]), reply[i + 1])
    }
    return map
  }

  function fieldNames(reply: RespWireValue): string[] {
    assert.ok(Array.isArray(reply))
    return reply.filter((_, i) => i % 2 === 0).map(respText)
  }

  /** Group `name`'s `entries-read` / `lag` pair, as wire bytes. */
  async function expectLag(
    k: string,
    entriesRead: number | string | null,
    lag: number | string | null,
    name = 'g',
  ): Promise<void> {
    conn.write(commandFrame('XINFO', 'GROUPS', k))
    const raw = (await conn.readRawFrame()).toString()
    const expected =
      `${bulk('entries-read')}${entriesRead === null ? NIL : int(entriesRead)}` +
      `${bulk('lag')}${lag === null ? NIL : int(lag)}`
    const row = raw.indexOf(`${bulk('name')}${bulk(name)}`)
    assert.ok(row >= 0, `group ${name} missing in ${JSON.stringify(raw)}`)
    const at = raw.indexOf(bulk('entries-read'), row)
    assert.strictEqual(
      raw.slice(at, at + expected.length),
      expected,
      `XINFO GROUPS ${k}: ${JSON.stringify(raw)}`,
    )
  }

  before(async () => {
    port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
  })

  after(async () => {
    if (used.length > 0) await send(conn, ['DEL', ...used])
    conn.close()
    await testRunner.cleanup()
  })

  describe('empty XREAD / XREADGROUP reply (#528, #519)', () => {
    test('is a null array on RESP2, also on a BLOCK timeout and inside MULTI', async () => {
      const k = await streamWithGroup('empty', 1)
      const missing = key('missing')
      await expectReply(
        conn,
        ['XREAD', 'COUNT', '5', 'STREAMS', k, '1-1'],
        NULL_ARRAY,
      )
      await expectReply(conn, ['XREAD', 'STREAMS', missing, '0'], NULL_ARRAY)
      await expectReply(
        conn,
        ['XREAD', 'BLOCK', '10', 'STREAMS', k, '$'],
        NULL_ARRAY,
      )
      await read(k, 'c')
      await expectReply(
        conn,
        ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', k, '>'],
        NULL_ARRAY,
      )
      await expectReply(
        conn,
        ['XREADGROUP', 'GROUP', 'g', 'c', 'BLOCK', '10', 'STREAMS', k, '>'],
        NULL_ARRAY,
      )

      await expectReply(conn, ['MULTI'], OK)
      await expectReply(conn, ['XREAD', 'STREAMS', k, '1-1'], '+QUEUED\r\n')
      await expectReply(
        conn,
        ['XREAD', 'BLOCK', '10', 'STREAMS', k, '$'],
        '+QUEUED\r\n',
      )
      await expectReply(
        conn,
        ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', k, '>'],
        '+QUEUED\r\n',
      )
      await expectReply(
        conn,
        ['EXEC'],
        `*3\r\n${NULL_ARRAY}${NULL_ARRAY}${NULL_ARRAY}`,
      )
    })

    test('a non-integer COUNT is the plain integer error', async () => {
      const k = await streamWithGroup('count', 1)
      await expectReply(
        conn,
        ['XREAD', 'COUNT', 'x', 'STREAMS', k, '0'],
        NOT_INTEGER,
      )
      await expectReply(
        conn,
        ['XREADGROUP', 'GROUP', 'g', 'c', 'COUNT', 'x', 'STREAMS', k, '>'],
        NOT_INTEGER,
      )
    })
  })

  describe('XREAD / XREADGROUP special ids (#519)', () => {
    test('XREAD + reads the last entry on Redis 7.4+ and Valkey 8.0+', async () => {
      const k = await streamWithGroup('plus', 2)
      const missing = key('plus-missing')
      if (plusId) {
        await expectReply(
          conn,
          ['XREAD', 'STREAMS', k, '+'],
          `*1\r\n*2\r\n${bulk(k)}*1\r\n*2\r\n${bulk('2-1')}*2\r\n${bulk('f')}${bulk('v')}`,
        )
        await expectReply(conn, ['XREAD', 'STREAMS', missing, '+'], NULL_ARRAY)
      } else {
        await expectReply(conn, ['XREAD', 'STREAMS', k, '+'], INVALID_ID)
      }
    })

    test('XREADGROUP refuses $ and + after the group check', async () => {
      const k = await streamWithGroup('group-ids', 1)
      const missing = key('group-ids-missing')
      await expectReply(
        conn,
        ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', k, '$'],
        meaningless('$'),
      )
      await expectReply(
        conn,
        ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', k, '+'],
        plusId ? meaningless('+') : INVALID_ID,
      )
      await expectReply(
        conn,
        ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', missing, '+'],
        `-NOGROUP No such key '${missing}' or consumer group 'g' in XREADGROUP with GROUP option\r\n`,
      )
    })
  })

  describe('listing order and reply shapes (#528, #495)', () => {
    test('consumers and groups are listed by name, XPENDING counts are bulk strings', async () => {
      const k = await streamWithGroup('order', 2)
      await expectReply(conn, ['XGROUP', 'CREATE', k, 'zz', '0'], OK)
      await expectReply(conn, ['XGROUP', 'CREATE', k, 'ag', '0'], OK)
      await read(k, 'c2')
      await read(k, 'c1')
      await expectReply(
        conn,
        ['XGROUP', 'CREATECONSUMER', k, 'g', 'Aa'],
        ':1\r\n',
      )

      const consumers = await command('XINFO', 'CONSUMERS', k, 'g')
      assert.ok(Array.isArray(consumers))
      assert.deepStrictEqual(
        consumers.map(row => respText(fields(row).get('name'))),
        ['Aa', 'c1', 'c2'],
      )

      const groups = await command('XINFO', 'GROUPS', k)
      assert.ok(Array.isArray(groups))
      assert.deepStrictEqual(
        groups.map(row => respText(fields(row).get('name'))),
        ['ag', 'g', 'zz'],
      )

      await expectReply(
        conn,
        ['XPENDING', k, 'g'],
        `*4\r\n:2\r\n${bulk('1-1')}${bulk('2-1')}*2\r\n` +
          `*2\r\n${bulk('c1')}${bulk('1')}*2\r\n${bulk('c2')}${bulk('1')}`,
      )
      await expectReply(
        conn,
        ['XPENDING', k, 'ag'],
        `*4\r\n:0\r\n${NIL}${NIL}${NULL_ARRAY}`,
      )
    })

    test('XINFO STREAM FULL lists delivery times and the consumer fields', async () => {
      const k = await streamWithGroup('full', 2)
      await expectReply(
        conn,
        ['XGROUP', 'CREATECONSUMER', k, 'g', 'c0'],
        ':1\r\n',
      )
      const t0 = Date.now()
      await read(k, 'c2')
      const t1 = Date.now()
      await read(k, 'c1')
      const t2 = Date.now()

      const reply = fields(await command('XINFO', 'STREAM', k, 'FULL'))
      const groups = reply.get('groups')
      assert.ok(Array.isArray(groups) && groups.length === 1)
      const group = fields(groups[0])

      const between = (value: RespWireValue, from: number, to: number) => {
        const ms = respNumber(value)
        assert.ok(from <= ms && ms <= to, `${ms} not in [${from}, ${to}]`)
      }

      const pel = group.get('pending')
      assert.ok(Array.isArray(pel) && pel.length === 2)
      const [first, second] = pel
      assert.ok(Array.isArray(first) && Array.isArray(second))
      assert.deepStrictEqual(
        [respText(first[0]), respText(first[1]), respNumber(first[3])],
        ['1-1', 'c2', 1],
      )
      between(first[2], t0, t1)
      assert.deepStrictEqual(
        [respText(second[0]), respText(second[1]), respNumber(second[3])],
        ['2-1', 'c1', 1],
      )
      between(second[2], t1, t2)

      const consumers = group.get('consumers')
      assert.ok(Array.isArray(consumers))
      const expectedFields = activeTime
        ? ['name', 'seen-time', 'active-time', 'pel-count', 'pending']
        : ['name', 'seen-time', 'pel-count', 'pending']
      for (const row of consumers) {
        assert.deepStrictEqual(fieldNames(row), expectedFields)
      }
      assert.deepStrictEqual(
        consumers.map(row => respText(fields(row).get('name'))),
        ['c0', 'c1', 'c2'],
      )

      const [c0, c1, c2] = consumers.map(fields)
      between(c0.get('seen-time') ?? null, 0, t0)
      if (activeTime) assert.strictEqual(c0.get('active-time'), -1)
      assert.strictEqual(c0.get('pel-count'), 0)
      assert.deepStrictEqual(c0.get('pending'), [])

      for (const [consumer, id, from, to] of [
        [c1, '2-1', t1, t2],
        [c2, '1-1', t0, t1],
      ] as const) {
        between(consumer.get('seen-time') ?? null, from, to)
        if (activeTime) between(consumer.get('active-time') ?? null, from, to)
        assert.strictEqual(consumer.get('pel-count'), 1)
        const rows = consumer.get('pending')
        assert.ok(Array.isArray(rows) && rows.length === 1)
        const row = rows[0]
        assert.ok(Array.isArray(row) && row.length === 3)
        assert.strictEqual(respText(row[0]), id)
        between(row[1], from, to)
        assert.strictEqual(row[2], 1)
      }
    })
  })

  describe('consumer-group lag (#528)', { skip: !groupLag }, () => {
    test('SETID with ENTRIESREAD, + and 0', async () => {
      const k = await streamWithGroup('setid', 2)
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '1-1', 'ENTRIESREAD', '7'],
        OK,
      )
      await expectLag(k, 7, -5)
      await expectReply(conn, ['XGROUP', 'SETID', k, 'g', '+'], OK)
      await expectLag(k, null, null)
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '0', 'ENTRIESREAD', '0'],
        OK,
      )
      await expectLag(k, 0, 2)
      await expectReply(conn, ['XGROUP', 'SETID', k, 'g', '0'], OK)
      await expectLag(k, null, 2)
    })

    test('a deletion ahead of the group, then a read past it', async () => {
      const k = await streamWithGroup('xdel-ahead', 3)
      await read(k, 'c')
      await send(conn, ['XDEL', k, '2-1'])
      await expectLag(k, 1, null)
      await read(k, 'c')
      if (tombstoneCheck) await expectLag(k, 3, 0)
      else await expectLag(k, 2, 1)
    })

    test('deleting the head, reading all, emptying, adding again', async () => {
      const k = await streamWithGroup('xdel-head', 3)
      await send(conn, ['XDEL', k, '1-1'])
      await expectLag(k, null, 2)
      await read(k, 'c', 10)
      await expectLag(k, 3, 0)
      await send(conn, ['XDEL', k, '2-1', '3-1'])
      await expectLag(k, 3, 0)
      await send(conn, ['XADD', k, '4-1', 'f', 'v'])
      await expectLag(k, 3, 1)
    })

    test('groups created at $, 0 and inside the stream', async () => {
      const k = key('created')
      await send(conn, ['XADD', k, '1-1', 'f', 'v'])
      await send(conn, ['XADD', k, '2-1', 'f', 'v'])
      await expectReply(conn, ['XGROUP', 'CREATE', k, 'g', '$'], OK)
      await send(conn, ['XADD', k, '3-1', 'f', 'v'])
      await expectLag(k, null, null)
      await expectReply(conn, ['XGROUP', 'CREATE', k, 'h', '0'], OK)
      await expectLag(k, null, 3, 'h')
      await expectReply(conn, ['XGROUP', 'CREATE', k, 'i', '2-1'], OK)
      await expectLag(k, null, null, 'i')
    })

    test('XCLAIM LASTID moves the last id but not the read counter', async () => {
      const k = await streamWithGroup('lastid', 2)
      await read(k, 'c')
      await send(conn, ['XCLAIM', k, 'g', 'c2', '0', '1-1', 'LASTID', '9-9'])
      await expectLag(k, 1, 1)
    })

    test('ENTRIESREAD and ENTRIESADDED take any int64', async () => {
      const k = await streamWithGroup('int64', 2)
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '0', 'ENTRIESREAD', INT64_MAX],
        OK,
      )
      await expectLag(
        k,
        INT64_MAX,
        lagShortcuts ? 2 : (2n - BigInt(INT64_MAX)).toString(),
      )
      await expectReply(
        conn,
        ['XGROUP', 'CREATE', k, 'g2', '0', 'ENTRIESREAD', '9007199254740993'],
        OK,
      )
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '0', 'ENTRIESREAD', '9223372036854775808'],
        NOT_INTEGER,
      )
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '0', 'ENTRIESREAD', '-1'],
        OK,
      )
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '0', 'ENTRIESREAD', '-2'],
        '-ERR value for ENTRIESREAD must be positive or -1\r\n',
      )

      await expectReply(
        conn,
        ['XSETID', k, '9-9', 'ENTRIESADDED', INT64_MAX, 'MAXDELETEDID', '0-1'],
        OK,
      )
      conn.write(commandFrame('XINFO', 'STREAM', k))
      const raw = (await conn.readRawFrame()).toString()
      assert.ok(raw.includes(`${bulk('entries-added')}${int(INT64_MAX)}`), raw)
    })
  })

  describe('implicit consumer creation event (#528)', () => {
    test('XREADGROUP / XCLAIM / XAUTOCLAIM announce it from 7.0, 6.2 does not', async () => {
      const readKey = await streamWithGroup('event-read', 1)
      const claimKey = await streamWithGroup('event-claim', 1)
      const autoKey = await streamWithGroup('event-auto', 1)
      const markerKey = await streamWithGroup('event-marker', 1)
      await read(claimKey, 'owner')
      await read(autoKey, 'owner')

      const saved = await command('CONFIG', 'GET', 'notify-keyspace-events')
      assert.ok(Array.isArray(saved))
      await expectReply(
        conn,
        ['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'],
        OK,
      )
      const sub = await RawRedisConnection.connect('127.0.0.1', port)
      try {
        sub.write(
          commandFrame('PSUBSCRIBE', '__keyevent@*__:xgroup-createconsumer'),
        )
        await sub.readFrame()

        await read(readKey, 'new-read')
        await send(conn, ['XCLAIM', claimKey, 'g', 'new-claim', '0', '1-1'])
        await send(conn, ['XAUTOCLAIM', autoKey, 'g', 'new-auto', '0', '0'])
        await expectReply(
          conn,
          ['XGROUP', 'CREATECONSUMER', markerKey, 'g', 'marker'],
          ':1\r\n',
        )

        const seen: string[] = []
        for (;;) {
          const message = await sub.readFrame()
          assert.ok(Array.isArray(message) && message.length === 4)
          assert.strictEqual(respText(message[0]), 'pmessage')
          seen.push(respText(message[3]))
          if (seen[seen.length - 1] === markerKey) break
        }
        assert.deepStrictEqual(
          seen,
          implicitEvent ? [readKey, claimKey, autoKey, markerKey] : [markerKey],
        )
      } finally {
        sub.close()
        await send(conn, [
          'CONFIG',
          'SET',
          'notify-keyspace-events',
          respText(saved[1]),
        ])
      }
    })
  })
})
