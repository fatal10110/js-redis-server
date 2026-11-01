import { describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as delay } from 'node:timers/promises'
import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
} from '../src/internal'

/** CLIENT TRACKING's table, driven through sessions (#252). */
function createServer() {
  const server = new RedisServerState({
    compatibility: 'redis-7.2',
    databaseCount: 16,
  })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  const connect = async (resp3 = true) => {
    const session = new ClientSession({ server, executor })
    if (resp3) {
      await session.execute('HELLO', [Buffer.from('3')])
    }
    return session
  }
  return { server, connect }
}

const run = (session: ClientSession, command: string, ...args: string[]) =>
  session.execute(
    command,
    args.map(arg => Buffer.from(arg)),
  )

/** The pushes queued for `session`, as wire bytes. */
const pushes = (session: ClientSession) =>
  session
    .takePushes()
    .map(push => push.encoded?.toString('latin1') ?? `<${push.value.kind}>`)

const invalidate = (key: string) =>
  `>2\r\n$10\r\ninvalidate\r\n*1\r\n$${key.length}\r\n${key}\r\n`
const invalidateAll = '>2\r\n$10\r\ninvalidate\r\n_\r\n'

describe('client tracking table', () => {
  test('FLUSHALL sends one null, not one per database', async () => {
    const { connect } = createServer()
    const tracker = await connect()
    const other = await connect(false)
    await run(tracker, 'CLIENT', 'TRACKING', 'ON')
    await run(tracker, 'GET', 'k')
    await run(other, 'FLUSHALL')
    assert.deepStrictEqual(pushes(tracker), [invalidateAll])
    await run(other, 'SET', 'k', '1')
    assert.deepStrictEqual(pushes(tracker), [])
  })

  test('an own write is delivered only once the reply is written', async () => {
    const { connect } = createServer()
    const tracker = await connect()
    await run(tracker, 'CLIENT', 'TRACKING', 'ON')
    await run(tracker, 'GET', 'k')
    const result = await run(tracker, 'SET', 'k', '1')
    assert.deepStrictEqual(pushes(tracker), [])
    result.options?.afterReply?.()
    assert.deepStrictEqual(pushes(tracker), [invalidate('k')])
  })

  test('a closed tracker is forgotten, and RESP2 trackers get nothing', async () => {
    const { server, connect } = createServer()
    const closed = await connect()
    const resp2 = await connect(false)
    const writer = await connect(false)
    for (const tracker of [closed, resp2]) {
      await run(tracker, 'CLIENT', 'TRACKING', 'ON', 'BCAST')
    }
    await closed.close()
    await run(writer, 'SET', 'k', '1')
    await delay(5)
    assert.deepStrictEqual(pushes(resp2), [])
    assert.strictEqual(server.clientTracking.info(closed).on, false)
    assert.strictEqual(server.clientTracking.info(resp2).bcast, true)
  })
})
