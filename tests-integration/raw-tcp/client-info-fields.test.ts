import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * CLIENT INFO / CLIENT LIST lines, INFO `blocked_clients` and the INFO
 * keyspace line over a bare socket, and the RESP3 type of all three replies
 * (#496, #32, #501).
 *
 * The field lists are `catClientInfoString`'s format in each version's
 * networking.c (redis 6.2.14 / 7.0.15 / 7.2.4 / 7.4.4 / 8.0.0, valkey 8.0.0
 * / 9.0.0); the real backend (Redis 8.0 in CI, or a local redis-server under
 * the matching REDIS_COMPAT) must print the same.
 */
const testRunner = new TestRunner()

const REDIS_70 =
  'id addr laddr fd name age idle flags db sub psub ssub multi qbuf qbuf-free argv-mem multi-mem rbs rbp obl oll omem tot-mem events cmd user redir resp'
const REDIS_74 =
  'id addr laddr fd name age idle flags db sub psub ssub multi watch qbuf qbuf-free argv-mem multi-mem rbs rbp obl oll omem tot-mem events cmd user redir resp lib-name lib-ver'
const CLIENT_FIELDS: Record<typeof activeProfile, string> = {
  'redis-6.2':
    'id addr laddr fd name age idle flags db sub psub multi qbuf qbuf-free argv-mem obl oll omem tot-mem events cmd user redir',
  'redis-7.0': REDIS_70,
  'redis-7.2': `${REDIS_70} lib-name lib-ver`,
  'redis-7.4': REDIS_74,
  'redis-8.0': `${REDIS_74} io-thread`,
  'valkey-8.0': `${REDIS_74} tot-net-in tot-net-out tot-cmds`,
  'valkey-9.0': REDIS_74.replace(' flags ', ' flags capa ').concat(
    ' tot-net-in tot-net-out tot-cmds',
  ),
}

/** `key=value` pairs of one client line, in order. */
function fields(line: string): Array<[string, string]> {
  return line
    .trimEnd()
    .split(' ')
    .map(field => {
      const at = field.indexOf('=')
      return [field.slice(0, at), field.slice(at + 1)]
    })
}

function field(line: string, name: string): string | undefined {
  return fields(line).find(([key]) => key === name)?.[1]
}

/** The body of a RESP2 bulk string reply. */
function bulkBody(reply: Buffer): string {
  const text = reply.toString()
  const match = /^\$(\d+)\r\n/.exec(text)
  assert.ok(match, `expected a bulk string, got ${JSON.stringify(text)}`)
  const body = text.slice(match[0].length, match[0].length + Number(match[1]))
  assert.strictEqual(text, `${match[0]}${body}\r\n`)
  return body
}

/** The body of a RESP3 `txt` verbatim string reply. */
function verbatimBody(reply: Buffer): string {
  const text = reply.toString()
  const match = /^=(\d+)\r\ntxt:/.exec(text)
  assert.ok(
    match,
    `expected a txt verbatim string, got ${JSON.stringify(text)}`,
  )
  const length = Number(match[1]) - 4
  const start = match[0].length
  const body = text.slice(start, start + length)
  assert.strictEqual(text, `${match[0]}${body}\r\n`)
  return body
}

describe(`Raw TCP CLIENT INFO / LIST and INFO (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  async function send(conn: RawRedisConnection, ...args: string[]) {
    conn.write(commandFrame(...args))
    return conn.readRawFrame()
  }

  async function blockedClients(conn: RawRedisConnection): Promise<number> {
    const info = bulkBody(await send(conn, 'INFO', 'clients'))
    const match = /^blocked_clients:(\d+)$/m.exec(info)
    assert.ok(match, info)
    return Number(match[1])
  }

  async function lineOf(observer: RawRedisConnection, id: string) {
    const list = bulkBody(await send(observer, 'CLIENT', 'LIST'))
    const line = list.split('\n').find(entry => entry.startsWith(`id=${id} `))
    assert.ok(line, `no CLIENT LIST line for id=${id}`)
    return line
  }

  test("CLIENT INFO prints the version's fields in order", async () => {
    const conn = await connect()
    const line = bulkBody(await send(conn, 'CLIENT', 'INFO'))
    assert.ok(line.endsWith('\n'), 'a client line ends in a newline')
    assert.strictEqual(
      fields(line)
        .map(([key]) => key)
        .join(' '),
      CLIENT_FIELDS[activeProfile],
    )

    // The server's end of the socket. A dockerized real server sees its own
    // container address and port there, not the published one we dialled.
    if (testRunner.backend === 'mock') {
      assert.strictEqual(field(line, 'laddr'), `127.0.0.1:${port}`)
    } else {
      assert.match(field(line, 'laddr') ?? '', /^\S+:\d+$/)
    }
    assert.match(field(line, 'fd') ?? '', /^\d+$/)
    assert.strictEqual(field(line, 'flags'), 'N')
    assert.strictEqual(field(line, 'multi'), '-1')
    assert.strictEqual(
      field(line, 'cmd'),
      activeProfile === 'redis-6.2' ? 'client' : 'client|info',
    )
  })

  test("CLIENT LIST shows another client's last command, MULTI state and blocking", async () => {
    const observer = await connect()
    const subject = await connect()
    const id = (await send(subject, 'CLIENT', 'ID')).toString().slice(1, -2)
    const key = `client-info:${randomKey()}`

    await send(subject, 'GET', key)
    assert.strictEqual(field(await lineOf(observer, id), 'cmd'), 'get')

    // Lookup failed: no last command.
    await send(subject, 'NOSUCHCOMMAND')
    assert.strictEqual(field(await lineOf(observer, id), 'cmd'), 'NULL')

    await send(subject, 'MULTI')
    await send(subject, 'GET', key)
    let line = await lineOf(observer, id)
    assert.strictEqual(field(line, 'flags'), 'x')
    assert.strictEqual(field(line, 'multi'), '1')
    await send(subject, 'DISCARD')

    const before = await blockedClients(observer)
    subject.write(commandFrame('BLPOP', key, '0'))
    const deadline = Date.now() + 5000
    while ((await blockedClients(observer)) !== before + 1) {
      assert.ok(Date.now() < deadline, 'BLPOP never counted as blocked')
      await delay(10)
    }
    line = await lineOf(observer, id)
    assert.strictEqual(field(line, 'flags'), 'b')
    assert.strictEqual(field(line, 'cmd'), 'blpop')

    await send(observer, 'RPUSH', key, 'v')
    assert.strictEqual(
      (await subject.readRawFrame()).toString(),
      `*2\r\n$${key.length}\r\n${key}\r\n$1\r\nv\r\n`,
    )
    assert.strictEqual(await blockedClients(observer), before)
    assert.strictEqual(field(await lineOf(observer, id), 'flags'), 'N')
  })

  test('INFO keyspace counts the keys with a TTL', async () => {
    const conn = await connect()
    const key = `client-info:${randomKey()}`
    await send(conn, 'SET', `${key}:a`, 'v', 'EX', '100')
    await send(conn, 'SET', `${key}:b`, 'v', 'EX', '100')

    const info = bulkBody(await send(conn, 'INFO', 'keyspace'))
    const extra =
      activeProfile === 'redis-7.4' || activeProfile === 'redis-8.0'
        ? ',subexpiry=\\d+'
        : activeProfile === 'valkey-9.0'
          ? ',keys_with_volatile_items=\\d+'
          : ''
    const match = new RegExp(
      `^db0:keys=(\\d+),expires=(\\d+),avg_ttl=(\\d+)${extra}$`,
      'm',
    ).exec(info)
    assert.ok(match, info)
    // Other suites' keys share the real server, so only lower bounds hold.
    assert.ok(Number(match[1]) >= 2, info)
    assert.ok(Number(match[2]) >= 2, info)
    await send(conn, 'DEL', `${key}:a`, `${key}:b`)
  })

  test('INFO, CLIENT INFO and CLIENT LIST are txt verbatim strings on RESP3', async () => {
    const conn = await connect()
    conn.write(commandFrame('HELLO', '3'))
    assert.ok((await conn.readFrame()) instanceof Map)

    assert.match(
      verbatimBody(await send(conn, 'INFO', 'server')),
      /^# Server\r\n/,
    )
    assert.strictEqual(verbatimBody(await send(conn, 'INFO', 'nosuch')), '')
    const line = verbatimBody(await send(conn, 'CLIENT', 'INFO'))
    if (activeProfile !== 'redis-6.2') {
      assert.strictEqual(field(line, 'resp'), '3')
    }
    assert.match(verbatimBody(await send(conn, 'CLIENT', 'LIST')), /^id=\d+ /)
  })
})
