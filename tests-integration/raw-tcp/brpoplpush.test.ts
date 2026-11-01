import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply, send } from './helpers'

/**
 * BRPOPLPUSH source destination timeout: the deprecated form of
 * `BLMOVE source destination RIGHT LEFT timeout` (#495). Every reply here is
 * the same on redis 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey 8.0.0 /
 * 9.0.0; the per-profile timeout rows live in
 * compatibility/numeric-edge-gates.test.ts and the script rows in
 * compatibility/profile-gates.test.ts.
 */
const testRunner = new TestRunner()

const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const ARITY = "-ERR wrong number of arguments for 'brpoplpush' command\r\n"

describe(`Raw TCP BRPOPLPUSH (${testRunner.getBackendName()})`, () => {
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

  test('BRPOPLPUSH moves the tail of source to the head of destination', async () => {
    const conn = await connect()
    const src = `brpoplpush:${randomKey()}:src`
    const dst = `brpoplpush:${randomKey()}:dst`
    const one = `brpoplpush:${randomKey()}:one`
    const same = `brpoplpush:${randomKey()}:same`

    await expectReply(conn, ['RPUSH', src, 'a', 'b', 'c'], ':3\r\n')
    await expectReply(conn, ['RPUSH', dst, 'x'], ':1\r\n')
    await expectReply(conn, ['BRPOPLPUSH', src, dst, '1'], '$1\r\nc\r\n')
    await expectReply(
      conn,
      ['LRANGE', src, '0', '-1'],
      '*2\r\n$1\r\na\r\n$1\r\nb\r\n',
    )
    await expectReply(
      conn,
      ['LRANGE', dst, '0', '-1'],
      '*2\r\n$1\r\nc\r\n$1\r\nx\r\n',
    )

    // Moving the last element deletes the source.
    await expectReply(conn, ['RPUSH', one, 'q'], ':1\r\n')
    await expectReply(conn, ['BRPOPLPUSH', one, dst, '0'], '$1\r\nq\r\n')
    await expectReply(conn, ['EXISTS', one], ':0\r\n')

    // Same key: a rotation.
    await expectReply(conn, ['RPUSH', same, '1', '2', '3'], ':3\r\n')
    await expectReply(conn, ['BRPOPLPUSH', same, same, '1'], '$1\r\n3\r\n')
    await expectReply(
      conn,
      ['LRANGE', same, '0', '-1'],
      '*3\r\n$1\r\n3\r\n$1\r\n1\r\n$1\r\n2\r\n',
    )
  })

  test('BRPOPLPUSH answers a null array on timeout (RESP2) and null (RESP3)', async () => {
    const conn = await connect()
    const missing = `brpoplpush:${randomKey()}:missing`
    const dst = `brpoplpush:${randomKey()}:dst`
    const stringKey = `brpoplpush:${randomKey()}:string`

    await expectReply(conn, ['BRPOPLPUSH', missing, dst, '0.01'], '*-1\r\n')
    await expectReply(conn, ['EXISTS', dst], ':0\r\n')
    // BLMOVE shares the reply (it used to answer a null bulk string).
    await expectReply(
      conn,
      ['BLMOVE', missing, dst, 'RIGHT', 'LEFT', '0.01'],
      '*-1\r\n',
    )

    // The destination type is only checked once an element moves.
    await expectReply(conn, ['SET', stringKey, 'v'], '+OK\r\n')
    await expectReply(
      conn,
      ['BRPOPLPUSH', missing, stringKey, '0.01'],
      '*-1\r\n',
    )

    const resp3 = await connect()
    await send(resp3, ['HELLO', '3'])
    await expectReply(resp3, ['BRPOPLPUSH', missing, dst, '0.01'], '_\r\n')
  })

  test('BRPOPLPUSH error paths match Redis', async () => {
    const conn = await connect()
    const src = `brpoplpush:${randomKey()}:src`
    const dst = `brpoplpush:${randomKey()}:dst`
    const stringKey = `brpoplpush:${randomKey()}:string`

    await expectReply(conn, ['RPUSH', src, 'x'], ':1\r\n')
    await expectReply(conn, ['BRPOPLPUSH', src, dst], ARITY)
    await expectReply(conn, ['BRPOPLPUSH', src, dst, '1', '2'], ARITY)
    await expectReply(
      conn,
      ['BRPOPLPUSH', src, dst, '-1'],
      '-ERR timeout is negative\r\n',
    )
    for (const timeout of ['abc', 'nan', '', ' 1']) {
      await expectReply(
        conn,
        ['BRPOPLPUSH', src, dst, timeout],
        '-ERR timeout is not a float or out of range\r\n',
      )
    }

    await expectReply(conn, ['SET', stringKey, 'value'], '+OK\r\n')
    await expectReply(conn, ['BRPOPLPUSH', stringKey, dst, '1'], WRONGTYPE)
    await expectReply(conn, ['BRPOPLPUSH', src, stringKey, '1'], WRONGTYPE)
    await expectReply(
      conn,
      ['BRPOPLPUSH', stringKey, stringKey, '1'],
      WRONGTYPE,
    )
    // Nothing moved by the failed calls.
    await expectReply(conn, ['LRANGE', src, '0', '-1'], '*1\r\n$1\r\nx\r\n')
    await expectReply(conn, ['EXISTS', dst], ':0\r\n')
  })

  test('BRPOPLPUSH inside MULTI never blocks', async () => {
    const conn = await connect()
    const missing = `brpoplpush:${randomKey()}:missing`
    const src = `brpoplpush:${randomKey()}:src`
    const dst = `brpoplpush:${randomKey()}:dst`

    await expectReply(conn, ['RPUSH', src, '1', '2'], ':2\r\n')
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['BRPOPLPUSH', missing, dst, '0'], '+QUEUED\r\n')
    await expectReply(conn, ['BRPOPLPUSH', src, dst, '0'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*2\r\n$-1\r\n$1\r\n2\r\n')

    const resp3 = await connect()
    await send(resp3, ['HELLO', '3'])
    await expectReply(resp3, ['MULTI'], '+OK\r\n')
    await expectReply(resp3, ['BRPOPLPUSH', missing, dst, '0'], '+QUEUED\r\n')
    await expectReply(resp3, ['EXEC'], '*1\r\n_\r\n')
  })

  test('a blocked BRPOPLPUSH is counted, flagged and woken by a push', async () => {
    const observer = await connect()
    const subject = await connect()
    const src = `brpoplpush:${randomKey()}:src`
    const dst = `brpoplpush:${randomKey()}:dst`

    const blockedClients = async (): Promise<number> => {
      observer.write(commandFrame('INFO', 'clients'))
      const info = (await observer.readRawFrame()).toString()
      const match = /^blocked_clients:(\d+)\r?$/m.exec(info)
      assert.ok(match, info)
      return Number(match[1])
    }
    const subjectFlags = async (id: string): Promise<string | undefined> => {
      observer.write(commandFrame('CLIENT', 'LIST'))
      const list = (await observer.readRawFrame()).toString()
      const line = list.split('\n').find(entry => entry.startsWith(`id=${id} `))
      assert.ok(line, `no CLIENT LIST line for id=${id}`)
      return / flags=(\S*)/.exec(line)?.[1]
    }

    subject.write(commandFrame('CLIENT', 'ID'))
    const id = (await subject.readRawFrame()).toString().slice(1, -2)
    const before = await blockedClients()
    subject.write(commandFrame('BRPOPLPUSH', src, dst, '0'))
    const deadline = Date.now() + 5000
    while ((await blockedClients()) !== before + 1) {
      assert.ok(Date.now() < deadline, 'BRPOPLPUSH never counted as blocked')
      await delay(10)
    }
    assert.strictEqual(await subjectFlags(id), 'b')

    await expectReply(observer, ['RPUSH', src, 'v'], ':1\r\n')
    assert.strictEqual((await subject.readRawFrame()).toString(), '$1\r\nv\r\n')
    await expectReply(observer, ['LRANGE', dst, '0', '-1'], '*1\r\n$1\r\nv\r\n')
    await expectReply(observer, ['EXISTS', src], ':0\r\n')
    assert.strictEqual(await blockedClients(), before)
    assert.strictEqual(await subjectFlags(id), 'N')
  })
})
