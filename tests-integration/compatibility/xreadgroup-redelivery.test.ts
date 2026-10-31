import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'

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
 * XREADGROUP history reads (any id but `>`) re-deliver the consumer's pending
 * entries (#624). For each entry still in the stream, real Redis bumps the
 * NACK's delivery count and resets its delivery time to the command time; a
 * deleted entry comes back as `[id, nil]` (a RESP2 null array) and its NACK is
 * left alone (t_stream.c streamReplyWithRangeFromConsumerPEL: 6.2.14 :1645,
 * 7.2.4 :1835, valkey 9.0.0 :1820). Same on every profile.
 *
 * Not mock-only: run it against a real server of any profile with
 * `TEST_BACKEND=real REDIS_STANDALONE_PORT=<port>`.
 */
const testRunner = new TestRunner()
const profile = activeProfile

const SLEEP_MS = 300
/** Generous bound for "reset to about 0" on a loaded CI box. */
const FRESH_MS = 200

function bulk(value: string): string {
  return `$${Buffer.byteLength(value)}\r\n${value}\r\n`
}

describe(`XREADGROUP history re-delivery (${testRunner.getBackendName()}, ${profile})`, () => {
  let conn: RawRedisConnection
  const RUN = randomKey()
  let counter = 0
  const used: string[] = []

  function key(label: string): string {
    const name = `redelivery:{${RUN}}:${label}:${counter++}`
    used.push(name)
    return name
  }

  async function command(...args: string[]): Promise<RespWireValue> {
    conn.write(commandFrame(...args))
    return conn.readFrame()
  }

  /** A stream with entries 1-1 .. n-1, group `g`, all delivered to `c`. */
  async function delivered(label: string, n: number): Promise<string> {
    const k = key(label)
    for (let i = 1; i <= n; i++)
      await send(conn, ['XADD', k, `${i}-1`, 'f', 'v'])
    await expectReply(conn, ['XGROUP', 'CREATE', k, 'g', '0'], '+OK\r\n')
    await send(conn, ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', k, '>'])
    return k
  }

  /** XPENDING's extended form as [id, idle ms, delivery count] rows. */
  async function pendingRows(k: string): Promise<[string, number, number][]> {
    const reply = await command('XPENDING', k, 'g', '-', '+', '10')
    assert.ok(Array.isArray(reply))
    return reply.map(row => {
      assert.ok(Array.isArray(row))
      assert.strictEqual(respText(row[1]), 'c')
      return [respText(row[0]), respNumber(row[2]), respNumber(row[3])]
    })
  }

  /** The value after field `name` in a flat RESP2 field/value array. */
  function fieldValue(reply: RespWireValue, name: string): RespWireValue {
    assert.ok(Array.isArray(reply))
    const at = reply.findIndex(
      (field, i) => i % 2 === 0 && respText(field) === name,
    )
    assert.ok(at >= 0, `missing field ${name}`)
    return reply[at + 1]
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

  test('a history read bumps the delivery count and resets the idle time', async () => {
    const k = await delivered('bump', 2)
    await sleep(SLEEP_MS)

    await expectReply(
      conn,
      ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', k, '0'],
      `*1\r\n*2\r\n${bulk(k)}*2\r\n` +
        `*2\r\n${bulk('1-1')}*2\r\n${bulk('f')}${bulk('v')}` +
        `*2\r\n${bulk('2-1')}*2\r\n${bulk('f')}${bulk('v')}`,
    )
    const rows = await pendingRows(k)
    assert.deepStrictEqual(
      rows.map(([id, , count]) => [id, count]),
      [
        ['1-1', 2],
        ['2-1', 2],
      ],
    )
    for (const [id, idle] of rows)
      assert.ok(idle < FRESH_MS, `${id} idle ${idle} not reset`)

    // COUNT bounds the re-delivery: only 1-1 is bumped again.
    await send(conn, [
      'XREADGROUP',
      'GROUP',
      'g',
      'c',
      'COUNT',
      '1',
      'STREAMS',
      k,
      '0',
    ])
    // Another consumer has nothing pending: nothing is re-delivered.
    await expectReply(
      conn,
      ['XREADGROUP', 'GROUP', 'g', 'other', 'STREAMS', k, '0'],
      `*1\r\n*2\r\n${bulk(k)}*0\r\n`,
    )
    assert.deepStrictEqual(
      (await pendingRows(k)).map(([id, , count]) => [id, count]),
      [
        ['1-1', 3],
        ['2-1', 2],
      ],
    )
  })

  test('a deleted entry comes back as a null array and keeps its NACK', async () => {
    const k = await delivered('deleted', 2)
    await send(conn, ['XDEL', k, '2-1'])
    await sleep(SLEEP_MS)

    for (let i = 0; i < 2; i++) {
      await expectReply(
        conn,
        ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', k, '0'],
        `*1\r\n*2\r\n${bulk(k)}*2\r\n` +
          `*2\r\n${bulk('1-1')}*2\r\n${bulk('f')}${bulk('v')}` +
          `*2\r\n${bulk('2-1')}*-1\r\n`,
      )
    }
    const [live, deleted] = await pendingRows(k)
    assert.strictEqual(live[0], '1-1')
    assert.strictEqual(live[2], 3)
    assert.ok(live[1] < FRESH_MS, `1-1 idle ${live[1]} not reset`)
    assert.strictEqual(deleted[0], '2-1')
    assert.strictEqual(deleted[2], 1)
    assert.ok(deleted[1] >= SLEEP_MS, `2-1 idle ${deleted[1]} was reset`)
  })

  test('XINFO STREAM FULL reports the bumped NACKs in both PEL views', async () => {
    const k = await delivered('full', 2)
    await send(conn, ['XDEL', k, '2-1'])
    await sleep(SLEEP_MS)
    const before = Date.now()
    await send(conn, ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', k, '0'])

    const info = await command('XINFO', 'STREAM', k, 'FULL')
    const groups = fieldValue(info, 'groups')
    assert.ok(Array.isArray(groups))
    const groupPel = fieldValue(groups[0], 'pending')
    assert.ok(Array.isArray(groupPel))
    const consumers = fieldValue(groups[0], 'consumers')
    assert.ok(Array.isArray(consumers))
    const consumerPel = fieldValue(consumers[0], 'pending')
    assert.ok(Array.isArray(consumerPel))

    // Group rows: [id, consumer, delivery time, count]; consumer rows:
    // [id, delivery time, count]. Both read the same NACK.
    const rows = [groupPel, consumerPel].map(pel =>
      pel.map(row => {
        assert.ok(Array.isArray(row))
        const tail = row.slice(-2)
        return [respText(row[0]), respNumber(tail[0]), respNumber(tail[1])]
      }),
    )
    for (const [
      [liveId, liveAt, liveCount],
      [deadId, deadAt, deadCount],
    ] of rows) {
      assert.deepStrictEqual([liveId, liveCount], ['1-1', 2])
      assert.deepStrictEqual([deadId, deadCount], ['2-1', 1])
      // Real-server clocks: allow a little skew against this process.
      assert.ok(liveAt >= before - 50, `1-1 delivery time ${liveAt} not reset`)
      assert.ok(
        deadAt < before - SLEEP_MS / 2,
        `2-1 delivery time ${deadAt} reset`,
      )
    }
  })
})
