import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * A client parked in a blocking command that hangs up is freed at once, and
 * the next push is not consumed on its behalf (#448, fixed by #554). The
 * BLPOP single-waiter case lives in client-info-fields.test.ts; this file
 * covers XREAD BLOCK and the hand-over to a second waiter.
 *
 * Every assertion goes through CLIENT LIST, INFO `blocked_clients`, LLEN and
 * XLEN, never through timing.
 */
const testRunner = new TestRunner()

function bulkBody(frame: Buffer): string {
  const text = frame.toString()
  const match = /^\$(\d+)\r\n/.exec(text)
  assert.ok(match, `not a bulk reply: ${JSON.stringify(text)}`)
  const start = match[0].length
  return text.slice(start, start + Number(match[1]))
}

describe(`Raw TCP blocked client disconnect (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  async function clientId(conn: RawRedisConnection): Promise<string> {
    return (await send(conn, 'CLIENT', 'ID')).toString().slice(1, -2)
  }

  async function blockedClients(conn: RawRedisConnection): Promise<number> {
    const info = bulkBody(await send(conn, 'INFO', 'clients'))
    const match = /^blocked_clients:(\d+)$/m.exec(info)
    assert.ok(match, info)
    return Number(match[1])
  }

  async function waitBlocked(
    observer: RawRedisConnection,
    expected: number,
    what: string,
  ): Promise<void> {
    const deadline = Date.now() + 5000
    while ((await blockedClients(observer)) !== expected) {
      assert.ok(Date.now() < deadline, `${what} never counted as blocked`)
      await delay(10)
    }
  }

  async function waitFreed(
    observer: RawRedisConnection,
    id: string,
    expectedBlocked: number,
  ): Promise<void> {
    const deadline = Date.now() + 2000
    for (;;) {
      const list = bulkBody(await send(observer, 'CLIENT', 'LIST'))
      const gone = !list.split('\n').some(l => l.startsWith(`id=${id} `))
      if (gone && (await blockedClients(observer)) === expectedBlocked) return
      assert.ok(
        Date.now() < deadline,
        `the disconnected client is still listed:\n${list}`,
      )
      await delay(10)
    }
  }

  const hangUps = [
    ['half-closes (EOF)', (conn: RawRedisConnection) => conn.end()],
    ['closes its socket', (conn: RawRedisConnection) => conn.close()],
  ] as const

  const kinds = [
    {
      name: 'BLPOP',
      park: (key: string) => commandFrame('BLPOP', key, '0'),
      push: (key: string) => ['RPUSH', key, 'v'],
      pushReply: ':1\r\n',
      length: 'LLEN',
      wake: (key: string) => `*2\r\n$${key.length}\r\n${key}\r\n$1\r\nv\r\n`,
    },
    {
      name: 'XREAD BLOCK',
      park: (key: string) =>
        commandFrame('XREAD', 'BLOCK', '0', 'STREAMS', key, '$'),
      push: (key: string) => ['XADD', key, '1-1', 'f', 'v'],
      pushReply: '$3\r\n1-1\r\n',
      length: 'XLEN',
      wake: (key: string) =>
        `*1\r\n*2\r\n$${key.length}\r\n${key}\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n`,
    },
  ] as const

  for (const kind of kinds) {
    for (const [how, hangUp] of hangUps) {
      // BLPOP + EOF/close is also pinned in client-info-fields.test.ts; the
      // XREAD BLOCK rows are the new ones, BLPOP stays for symmetry.
      test(`a client blocked in ${kind.name} that ${how} is freed and the push stays`, async () => {
        const observer = await connect()
        const subject = await connect()
        const id = await clientId(subject)
        const key = `disconnect:${randomKey()}`

        const before = await blockedClients(observer)
        subject.write(kind.park(key))
        await waitBlocked(observer, before + 1, kind.name)

        hangUp(subject)
        await waitFreed(observer, id, before)

        assert.strictEqual(
          (await send(observer, ...kind.push(key))).toString(),
          kind.pushReply,
        )
        assert.strictEqual(
          (await send(observer, kind.length, key)).toString(),
          ':1\r\n',
        )
        await send(observer, 'DEL', key)
      })

      test(`${kind.name}: the first of two waiters ${how}, the push goes to the second`, async () => {
        const observer = await connect()
        const first = await connect()
        const second = await connect()
        const pusher = await connect()
        const firstId = await clientId(first)
        const key = `disconnect:${randomKey()}`

        // Park in a known order: the first is blocked before the second sends.
        const before = await blockedClients(observer)
        first.write(kind.park(key))
        await waitBlocked(observer, before + 1, `first ${kind.name}`)
        second.write(kind.park(key))
        await waitBlocked(observer, before + 2, `second ${kind.name}`)

        hangUp(first)
        await waitFreed(observer, firstId, before + 1)

        assert.strictEqual(
          (await send(pusher, ...kind.push(key))).toString(),
          kind.pushReply,
        )
        assert.strictEqual(
          (await second.readRawFrame()).toString(),
          kind.wake(key),
        )
        assert.strictEqual(await blockedClients(observer), before)
        if (kind.name === 'BLPOP') {
          // The waiter consumed the element.
          assert.strictEqual(
            (await send(observer, 'LLEN', key)).toString(),
            ':0\r\n',
          )
        } else {
          // XREAD does not consume: the entry stays in the stream.
          assert.strictEqual(
            (await send(observer, 'XLEN', key)).toString(),
            ':1\r\n',
          )
        }
        await send(observer, 'DEL', key)
      })
    }
  }
})
