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
 * Consumer-group error paths and version deltas (#507, #498).
 *
 * Like `unknown-subcommand-dispatch.test.ts` this suite is not mock-only:
 * every expectation follows `REDIS_COMPAT`, so it also runs against a real
 * server of the matching version (`TEST_BACKEND=real
 * REDIS_STANDALONE_PORT=<port>`). Checked against redis-server 6.2.24
 * (`redis-6.2`), 7.0.15 (`redis-7.0`), 7.2.4 (`redis-7.2`), 8.0.6
 * (`redis-8.0`) and Valkey 8.0 / 9.0.
 *
 * Raw TCP: the exact reply bytes are what is under test, and MULTI has to be
 * interleaved command by command.
 */
const testRunner = new TestRunner()
const profile = activeProfile

/** 7.0+ / every Valkey: ENTRIESREAD, `entries-read` / `lag`, 7.0 XINFO STREAM fields. */
const groupLag = profile !== 'redis-6.2'
/** 7.0+: XCLAIM / XAUTOCLAIM drop a deleted entry from the PEL. */
const dropsDeleted = profile !== 'redis-6.2'
/** 7.2+: `inactive`, and consumers created before anything is claimed. */
const activeTime = profile !== 'redis-6.2' && profile !== 'redis-7.0'

const NO_SUCH_KEY = '-ERR no such key\r\n'
const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const XGROUP_MISSING_KEY =
  '-ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.\r\n'
const OK = '+OK\r\n'

function syntaxError(container: string, subcommand: string): string {
  const lead = profile === 'redis-6.2' ? 'Unknown' : 'unknown'
  return `-ERR ${lead} subcommand or wrong number of arguments for '${subcommand}'. Try ${container} HELP.\r\n`
}

function noGroup(key: string, group: string): string {
  return `-NOGROUP No such consumer group '${group}' for key name '${key}'\r\n`
}

function bulk(value: string): string {
  return `$${Buffer.byteLength(value)}\r\n${value}\r\n`
}

function int(value: number): string {
  return `:${value}\r\n`
}

const NIL = '$-1\r\n'

type GroupRow = {
  name: string
  consumers: number
  pending: number
  lastId: string
  entriesRead: number | null
  lag: number
}

/** An XINFO GROUPS reply, with `entries-read` / `lag` on 7.0+. */
function groupsReply(rows: GroupRow[]): string {
  return `*${rows.length}\r\n${rows
    .map(row => {
      const fields = [
        bulk('name'),
        bulk(row.name),
        bulk('consumers'),
        int(row.consumers),
        bulk('pending'),
        int(row.pending),
        bulk('last-delivered-id'),
        bulk(row.lastId),
      ]
      if (groupLag) {
        fields.push(
          bulk('entries-read'),
          row.entriesRead === null ? NIL : int(row.entriesRead),
          bulk('lag'),
          int(row.lag),
        )
      }
      return `*${fields.length}\r\n${fields.join('')}`
    })
    .join('')}`
}

function entry(id: string): string {
  return `*2\r\n${bulk(id)}*2\r\n${bulk('f')}${bulk('v')}`
}

describe(`stream consumer-group error paths and version gates (${testRunner.getBackendName()}, ${profile})`, () => {
  let conn: RawRedisConnection
  const RUN = randomKey()
  let counter = 0
  const used: string[] = []

  /** A fresh key for this run, deleted in `after`. */
  function key(label: string): string {
    const name = `streamgates:{${RUN}}:${label}:${counter++}`
    used.push(name)
    return name
  }

  async function command(...args: string[]): Promise<RespWireValue> {
    conn.write(commandFrame(...args))
    return conn.readFrame()
  }

  /** A stream `k` with entries 1-1 .. n-1 and group `g` at 0. */
  async function streamWithGroup(label: string, n: number): Promise<string> {
    const k = key(label)
    for (let i = 1; i <= n; i++)
      await send(conn, ['XADD', k, `${i}-1`, 'f', 'v'])
    await expectReply(conn, ['XGROUP', 'CREATE', k, 'g', '0'], OK)
    return k
  }

  /** Group `g`'s consumers, sorted (the listing order is not under test). */
  async function consumerNames(k: string): Promise<string[]> {
    const reply = await command('XINFO', 'CONSUMERS', k, 'g')
    assert.ok(Array.isArray(reply))
    return reply
      .map(row => {
        assert.ok(Array.isArray(row))
        return respText(row[1])
      })
      .sort()
  }

  /** Group `g`'s `last-delivered-id`, the XINFO GROUPS row's 8th element. */
  async function lastDeliveredId(k: string): Promise<string> {
    const reply = await command('XINFO', 'GROUPS', k)
    assert.ok(Array.isArray(reply) && Array.isArray(reply[0]))
    assert.strictEqual(respText(reply[0][6]), 'last-delivered-id')
    return respText(reply[0][7])
  }

  /** XPENDING's extended form as [id, consumer, delivery count] rows. */
  async function pendingRows(k: string): Promise<[string, string, number][]> {
    const reply = await command('XPENDING', k, 'g', '-', '+', '10')
    assert.ok(Array.isArray(reply))
    return reply.map(row => {
      assert.ok(Array.isArray(row))
      return [respText(row[0]), respText(row[1]), respNumber(row[3])]
    })
  }

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
  })

  after(async () => {
    if (used.length > 0) await send(conn, ['DEL', ...used])
    conn.close()
    await testRunner.cleanup()
  })

  describe('XINFO STREAM reads its options after the key (#507)', () => {
    test('a missing or wrong-type key beats a bad option', async () => {
      const missing = key('missing')
      const string = key('string')
      await send(conn, ['SET', string, 'x'])

      await expectReply(conn, ['XINFO', 'STREAM', missing, 'x'], NO_SUCH_KEY)
      await expectReply(
        conn,
        ['XINFO', 'STREAM', missing, 'FULL', 'COUNT', 'abc'],
        NO_SUCH_KEY,
      )
      await expectReply(conn, ['XINFO', 'STREAM', string, 'x'], WRONGTYPE)
      await expectReply(
        conn,
        ['XINFO', 'STREAM', string, 'FULL', 'COUNT', 'abc'],
        WRONGTYPE,
      )
    })

    test('on a stream a bad option is the subcommand syntax error', async () => {
      const k = key('stream')
      await send(conn, ['XADD', k, '1-1', 'f', 'v'])

      await expectReply(
        conn,
        ['XINFO', 'STREAM', k, 'x'],
        syntaxError('XINFO', 'STREAM'),
      )
      await expectReply(
        conn,
        ['XINFO', 'stream', k, 'FULL', 'COUNT'],
        syntaxError('XINFO', 'stream'),
      )
      await expectReply(
        conn,
        ['XINFO', 'STREAM', k, 'FULL', 'COUNT', 'abc'],
        '-ERR value is not an integer or out of range\r\n',
      )
    })

    test('FULL COUNT: negative is the default 10, 0 is everything', async () => {
      const k = key('full')
      const ids: string[] = []
      for (let i = 1; i <= 12; i++) {
        ids.push(`${i}-1`)
        await send(conn, ['XADD', k, `${i}-1`, 'f', 'v'])
      }

      // `entries` comes before `groups`, as in real Redis.
      const full = (count: number): string => {
        const fields = [
          bulk('length'),
          int(12),
          bulk('radix-tree-keys'),
          int(1),
          bulk('radix-tree-nodes'),
          int(2),
          bulk('last-generated-id'),
          bulk('12-1'),
        ]
        if (groupLag) {
          fields.push(
            bulk('max-deleted-entry-id'),
            bulk('0-0'),
            bulk('entries-added'),
            int(12),
            bulk('recorded-first-entry-id'),
            bulk('1-1'),
          )
        }
        const listed = ids.slice(0, count)
        fields.push(
          bulk('entries'),
          `*${listed.length}\r\n${listed.map(entry).join('')}`,
          bulk('groups'),
          '*0\r\n',
        )
        return `*${fields.length}\r\n${fields.join('')}`
      }

      await expectReply(
        conn,
        ['XINFO', 'STREAM', k, 'FULL', 'COUNT', '-1'],
        full(10),
      )
      await expectReply(
        conn,
        ['XINFO', 'STREAM', k, 'FULL', 'COUNT', '0'],
        full(12),
      )
      await expectReply(
        conn,
        ['XINFO', 'STREAM', k, 'FULL', 'COUNT', '2'],
        full(2),
      )
      await expectReply(conn, ['XINFO', 'STREAM', k, 'FULL'], full(10))
    })

    test('the 7.0 stream fields are absent on 6.2', async () => {
      const k = key('fields')
      await send(conn, ['XADD', k, '1-1', 'f', 'v'])
      await send(conn, ['XADD', k, '2-1', 'f', 'v'])
      await send(conn, ['XDEL', k, '1-1'])

      const fields = [
        bulk('length'),
        int(1),
        bulk('radix-tree-keys'),
        int(1),
        bulk('radix-tree-nodes'),
        int(2),
        bulk('last-generated-id'),
        bulk('2-1'),
      ]
      if (groupLag) {
        fields.push(
          bulk('max-deleted-entry-id'),
          bulk('1-1'),
          bulk('entries-added'),
          int(2),
          bulk('recorded-first-entry-id'),
          bulk('2-1'),
        )
      }
      fields.push(
        bulk('groups'),
        int(0),
        bulk('first-entry'),
        entry('2-1'),
        bulk('last-entry'),
        entry('2-1'),
      )
      await expectReply(
        conn,
        ['XINFO', 'STREAM', k],
        `*${fields.length}\r\n${fields.join('')}`,
      )
    })
  })

  describe('XGROUP checks the key and group before the id (#507)', () => {
    test('every subcommand but CREATE MKSTREAM needs the key to exist', async () => {
      const missing = key('missing')
      for (const args of [
        ['SETID', missing, 'g', '0'],
        ['SETID', missing, 'g', 'bad'],
        ['DESTROY', missing, 'g'],
        ['CREATECONSUMER', missing, 'g', 'c'],
        ['DELCONSUMER', missing, 'g', 'c'],
        ['CREATE', missing, 'g', 'bad'],
      ]) {
        await expectReply(conn, ['XGROUP', ...args], XGROUP_MISSING_KEY)
      }
      await expectReply(
        conn,
        ['XGROUP', 'CREATE', missing, 'g', 'bad', 'MKSTREAM'],
        '-ERR Invalid stream ID specified as stream command argument\r\n',
      )
      await expectReply(conn, ['EXISTS', missing], ':0\r\n')

      const string = key('string')
      await send(conn, ['SET', string, 'x'])
      await expectReply(
        conn,
        ['XGROUP', 'CREATE', string, 'g', 'bad'],
        WRONGTYPE,
      )
      await expectReply(conn, ['XGROUP', 'DESTROY', string, 'g'], WRONGTYPE)
    })

    test('a missing group is NOGROUP in XGROUP wording, 0 for DESTROY', async () => {
      const k = await streamWithGroup('nogroup', 1)
      for (const args of [
        ['SETID', k, 'nog', '0'],
        ['SETID', k, 'nog', 'bad'],
        ['CREATECONSUMER', k, 'nog', 'c'],
        ['DELCONSUMER', k, 'nog', 'c'],
      ]) {
        await expectReply(conn, ['XGROUP', ...args], noGroup(k, 'nog'))
      }
      await expectReply(conn, ['XGROUP', 'DESTROY', k, 'nog'], ':0\r\n')
    })

    test('SETID takes - and + as ids; CREATE does not', async () => {
      const k = await streamWithGroup('setid-range', 2)
      await expectReply(conn, ['XGROUP', 'SETID', k, 'g', '+'], OK)
      assert.strictEqual(
        await lastDeliveredId(k),
        '18446744073709551615-18446744073709551615',
      )
      await expectReply(conn, ['XGROUP', 'SETID', k, 'g', '-'], OK)
      await expectReply(
        conn,
        ['XINFO', 'GROUPS', k],
        groupsReply([
          {
            name: 'g',
            consumers: 0,
            pending: 0,
            lastId: '0-0',
            entriesRead: null,
            lag: 2,
          },
        ]),
      )
      await expectReply(
        conn,
        ['XGROUP', 'CREATE', k, 'g2', '-'],
        '-ERR Invalid stream ID specified as stream command argument\r\n',
      )
    })

    test('a bad option is a syntax error before the key is looked up', async () => {
      const missing = key('missing')
      await expectReply(
        conn,
        ['XGROUP', 'CREATE', missing, 'g', '0', 'FOO'],
        syntaxError('XGROUP', 'CREATE'),
      )
    })

    test('a MULTI queues option errors and EXEC reports them', async () => {
      const k = key('multi')
      await send(conn, ['XADD', k, '1-1', 'f', 'v'])
      await expectReply(conn, ['MULTI'], OK)
      await expectReply(conn, ['XINFO', 'STREAM', k, 'x'], '+QUEUED\r\n')
      await expectReply(
        conn,
        ['XGROUP', 'CREATE', k, 'g', '0', 'FOO'],
        '+QUEUED\r\n',
      )
      await expectReply(
        conn,
        ['XINFO', 'STREAM', k, 'FULL', 'COUNT', 'abc'],
        '+QUEUED\r\n',
      )
      await expectReply(
        conn,
        ['EXEC'],
        `*3\r\n${syntaxError('XINFO', 'STREAM')}${syntaxError('XGROUP', 'CREATE')}-ERR value is not an integer or out of range\r\n`,
      )
    })
  })

  describe('XGROUP ENTRIESREAD (#507)', () => {
    test('7.0+: -1 is accepted as "unknown", below -1 is refused', async () => {
      if (!groupLag) return
      const k = key('entriesread')
      await send(conn, ['XADD', k, '1-1', 'f', 'v'])
      await send(conn, ['XADD', k, '2-1', 'f', 'v'])

      await expectReply(
        conn,
        ['XGROUP', 'CREATE', k, 'g', '0', 'ENTRIESREAD', '-1'],
        OK,
      )
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '0', 'ENTRIESREAD', '-1'],
        OK,
      )
      await expectReply(
        conn,
        ['XINFO', 'GROUPS', k],
        groupsReply([
          {
            name: 'g',
            consumers: 0,
            pending: 0,
            lastId: '0-0',
            entriesRead: null,
            lag: 2,
          },
        ]),
      )
      const refused = '-ERR value for ENTRIESREAD must be positive or -1\r\n'
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '0', 'ENTRIESREAD', '-2'],
        refused,
      )
      // Read before the key, so it beats both a missing key and WRONGTYPE.
      await expectReply(
        conn,
        ['XGROUP', 'SETID', key('missing'), 'g', '0', 'ENTRIESREAD', '-5'],
        refused,
      )
      await expectReply(
        conn,
        [
          'XGROUP',
          'CREATE',
          key('missing'),
          'g',
          '0',
          'MKSTREAM',
          'ENTRIESREAD',
          '-3',
        ],
        refused,
      )
    })

    test('7.0+: the options repeat, but only up to the argument limit', async () => {
      if (!groupLag) return
      const k = await streamWithGroup('entriesread-argc', 1)
      await expectReply(
        conn,
        ['XGROUP', 'CREATE', k, 'g2', '0', 'MKSTREAM', 'MKSTREAM'],
        OK,
      )
      await expectReply(
        conn,
        [
          'XGROUP',
          'CREATE',
          k,
          'g3',
          '0',
          'MKSTREAM',
          'MKSTREAM',
          'MKSTREAM',
          'MKSTREAM',
        ],
        syntaxError('XGROUP', 'CREATE'),
      )
      await expectReply(
        conn,
        [
          'XGROUP',
          'SETID',
          k,
          'g',
          '0',
          'ENTRIESREAD',
          '1',
          'ENTRIESREAD',
          '2',
        ],
        syntaxError('XGROUP', 'SETID'),
      )
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '0', 'MKSTREAM'],
        syntaxError('XGROUP', 'SETID'),
      )
    })

    test('6.2: ENTRIESREAD is not an option', async () => {
      if (groupLag) return
      const k = await streamWithGroup('entriesread-62', 1)
      await expectReply(
        conn,
        ['XGROUP', 'CREATE', k, 'g2', '0', 'ENTRIESREAD', '1'],
        syntaxError('XGROUP', 'CREATE'),
      )
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'g', '0', 'ENTRIESREAD', '1'],
        syntaxError('XGROUP', 'SETID'),
      )
      // The key and group are checked first.
      await expectReply(
        conn,
        ['XGROUP', 'SETID', k, 'nog', 'bad', 'ENTRIESREAD', '1'],
        noGroup(k, 'nog'),
      )
      await expectReply(
        conn,
        [
          'XGROUP',
          'CREATE',
          key('missing'),
          'g',
          '0',
          'ENTRIESREAD',
          '3',
          'MKSTREAM',
        ],
        XGROUP_MISSING_KEY,
      )
      await expectReply(
        conn,
        ['XGROUP', 'CREATE', k, 'g2', '0', 'MKSTREAM', 'MKSTREAM'],
        syntaxError('XGROUP', 'CREATE'),
      )
    })
  })

  describe('XCLAIM (#498)', () => {
    test('a LASTID behind the group is ignored, one ahead moves it', async () => {
      const k = await streamWithGroup('lastid', 3)
      await send(conn, [
        'XREADGROUP',
        'GROUP',
        'g',
        'c1',
        'COUNT',
        '2',
        'STREAMS',
        k,
        '>',
      ])

      await expectReply(
        conn,
        ['XCLAIM', k, 'g', 'c2', '0', '1-1', 'LASTID', '0-1', 'JUSTID'],
        `*1\r\n${bulk('1-1')}`,
      )
      await expectReply(
        conn,
        ['XINFO', 'GROUPS', k],
        groupsReply([
          {
            name: 'g',
            consumers: 2,
            pending: 2,
            lastId: '2-1',
            entriesRead: 2,
            lag: 1,
          },
        ]),
      )

      await send(conn, [
        'XCLAIM',
        k,
        'g',
        'c2',
        '0',
        '1-1',
        'LASTID',
        '9-1',
        'JUSTID',
      ])
      assert.strictEqual(await lastDeliveredId(k), '9-1')
    })

    test('FORCE adds a missing entry as one delivery, regardless of min-idle-time', async () => {
      const k = await streamWithGroup('force', 3)
      await send(conn, [
        'XREADGROUP',
        'GROUP',
        'g',
        'c1',
        'COUNT',
        '1',
        'STREAMS',
        k,
        '>',
      ])

      await expectReply(
        conn,
        ['XCLAIM', k, 'g', 'c2', '100000', '2-1', 'FORCE', 'JUSTID'],
        `*1\r\n${bulk('2-1')}`,
      )
      // min-idle-time still applies to an entry that already was pending.
      await expectReply(
        conn,
        ['XCLAIM', k, 'g', 'c3', '100000', '1-1', 'FORCE', 'JUSTID'],
        '*0\r\n',
      )
      await expectReply(
        conn,
        ['XCLAIM', k, 'g', 'c3', '0', '3-1', 'FORCE'],
        `*1\r\n${entry('3-1')}`,
      )
      // A FORCE on an id the stream does not hold claims nothing.
      await expectReply(
        conn,
        ['XCLAIM', k, 'g', 'c3', '0', '9-9', 'FORCE'],
        '*0\r\n',
      )
      assert.deepStrictEqual(await pendingRows(k), [
        ['1-1', 'c1', 1],
        ['2-1', 'c2', 1],
        ['3-1', 'c3', 2],
      ])
    })

    test('a deleted entry: dropped from 7.0, claimed as nil on 6.2', async () => {
      const k = await streamWithGroup('deleted', 2)
      await send(conn, ['XREADGROUP', 'GROUP', 'g', 'c1', 'STREAMS', k, '>'])
      await send(conn, ['XDEL', k, '1-1'])

      if (dropsDeleted) {
        await expectReply(conn, ['XCLAIM', k, 'g', 'c2', '0', '1-1'], '*0\r\n')
        assert.deepStrictEqual(await pendingRows(k), [['2-1', 'c1', 1]])
        return
      }

      await expectReply(
        conn,
        ['XCLAIM', k, 'g', 'c2', '0', '1-1'],
        `*1\r\n${NIL}`,
      )
      await expectReply(
        conn,
        ['XCLAIM', k, 'g', 'c2', '0', '1-1', 'JUSTID'],
        `*1\r\n${bulk('1-1')}`,
      )
      assert.deepStrictEqual(await pendingRows(k), [
        ['1-1', 'c2', 2],
        ['2-1', 'c1', 1],
      ])
    })

    test('the consumer is created up front from 7.2, only on a claim before', async () => {
      const k = await streamWithGroup('consumer', 1)
      await send(conn, ['XREADGROUP', 'GROUP', 'g', 'c1', 'STREAMS', k, '>'])

      await expectReply(
        conn,
        ['XCLAIM', k, 'g', 'idle', '999999999', '1-1'],
        '*0\r\n',
      )
      await expectReply(
        conn,
        ['XCLAIM', k, 'g', 'none', '0', '9-9', 'FORCE'],
        '*0\r\n',
      )
      await send(conn, ['XAUTOCLAIM', k, 'g', 'auto', '999999999', '0'])
      // Nothing to deliver. (The null reply itself is not under test here.)
      await send(conn, ['XREADGROUP', 'GROUP', 'g', 'empty', 'STREAMS', k, '>'])
      assert.deepStrictEqual(
        await consumerNames(k),
        activeTime ? ['auto', 'c1', 'empty', 'idle', 'none'] : ['c1'],
      )
    })
  })

  describe('XAUTOCLAIM (#498)', () => {
    test('COUNT is capped at LONG_MAX / 16 from 7.0, LONG_MAX on 6.2', async () => {
      const k = await streamWithGroup('count', 1)
      const tooHigh = '576460752303423488' // LONG_MAX / 16 + 1
      if (dropsDeleted) {
        await expectReply(
          conn,
          ['XAUTOCLAIM', k, 'g', 'c', '0', '0', 'COUNT', tooHigh],
          '-ERR COUNT must be > 0\r\n',
        )
        return
      }
      await expectReply(
        conn,
        ['XAUTOCLAIM', k, 'g', 'c', '0', '0', 'COUNT', '9223372036854775807'],
        `*2\r\n${bulk('0-0')}*0\r\n`,
      )
    })

    test('at most COUNT * 10 pending entries are examined; the cursor follows the last', async () => {
      const k = await streamWithGroup('attempts', 25)
      await send(conn, ['XREADGROUP', 'GROUP', 'g', 'c1', 'STREAMS', k, '>'])
      const tail = dropsDeleted ? '*0\r\n*0\r\n' : '*0\r\n'
      const head = dropsDeleted ? '*3\r\n' : '*2\r\n'

      await expectReply(
        conn,
        ['XAUTOCLAIM', k, 'g', 'c2', '999999', '0', 'COUNT', '1'],
        `${head}${bulk('11-1')}${tail}`,
      )
      await expectReply(
        conn,
        ['XAUTOCLAIM', k, 'g', 'c2', '999999', '0', 'COUNT', '2'],
        `${head}${bulk('21-1')}${tail}`,
      )
      // Twenty from 6-1 reach the end: the cursor wraps to 0-0.
      await expectReply(
        conn,
        ['XAUTOCLAIM', k, 'g', 'c2', '999999', '(5-1', 'COUNT', '2'],
        `${head}${bulk('0-0')}${tail}`,
      )
    })

    test('deleted entries count towards COUNT from 7.0; the cursor is the next pending id', async () => {
      const k = await streamWithGroup('auto-deleted', 5)
      await send(conn, ['XREADGROUP', 'GROUP', 'g', 'c1', 'STREAMS', k, '>'])
      await send(conn, ['XDEL', k, '2-1', '3-1'])

      if (dropsDeleted) {
        await expectReply(
          conn,
          ['XAUTOCLAIM', k, 'g', 'c2', '0', '0', 'COUNT', '2', 'JUSTID'],
          `*3\r\n${bulk('3-1')}*1\r\n${bulk('1-1')}*1\r\n${bulk('2-1')}`,
        )
        await expectReply(
          conn,
          ['XAUTOCLAIM', k, 'g', 'c2', '0', '0', 'COUNT', '2', 'JUSTID'],
          `*3\r\n${bulk('4-1')}*1\r\n${bulk('1-1')}*1\r\n${bulk('3-1')}`,
        )
        return
      }

      await expectReply(
        conn,
        ['XAUTOCLAIM', k, 'g', 'c2', '0', '0', 'COUNT', '2'],
        `*2\r\n${bulk('3-1')}*2\r\n${entry('1-1')}${NIL}`,
      )
    })
  })

  describe('XINFO GROUPS / CONSUMERS fields (#498)', () => {
    test('entries-read and lag from 7.0, inactive from 7.2', async () => {
      const k = await streamWithGroup('fields', 2)
      await send(conn, [
        'XREADGROUP',
        'GROUP',
        'g',
        'c1',
        'COUNT',
        '1',
        'STREAMS',
        k,
        '>',
      ])
      await expectReply(
        conn,
        ['XINFO', 'GROUPS', k],
        groupsReply([
          {
            name: 'g',
            consumers: 1,
            pending: 1,
            lastId: '1-1',
            entriesRead: 1,
            lag: 1,
          },
        ]),
      )

      const consumers = await command('XINFO', 'CONSUMERS', k, 'g')
      assert.ok(Array.isArray(consumers) && Array.isArray(consumers[0]))
      const names = consumers[0].filter((_, i) => i % 2 === 0).map(respText)
      assert.deepStrictEqual(
        names,
        activeTime
          ? ['name', 'pending', 'idle', 'inactive']
          : ['name', 'pending', 'idle'],
      )
    })
  })
})
