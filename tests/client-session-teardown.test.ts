import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
} from '../src/internal'

function createSession() {
  const server = new RedisServerState({ databaseCount: 2 })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  return { server, session: new ClientSession({ server, executor }) }
}

function buffers(...values: string[]): Buffer[] {
  return values.map(value => Buffer.from(value))
}

/** Registers three onReset hooks, the middle one throwing. */
function registerHooks(session: ClientSession): string[] {
  const ran: string[] = []
  session.onReset(() => ran.push('first'))
  session.onReset(() => {
    ran.push('throws')
    throw new Error('hook failed')
  })
  session.onReset(() => ran.push('last'))
  return ran
}

/**
 * A throwing onReset hook cannot cut teardown short (#509): close() and RESET
 * run every other hook and finish their own steps.
 */
describe('ClientSession teardown with a throwing onReset hook', () => {
  test('close() runs every hook and completes teardown', async () => {
    const { session } = createSession()
    await session.execute('subscribe', buffers('a'))
    await session.execute('watch', buffers('k'))
    await session.execute('multi', [])
    await session.execute('set', buffers('k', 'v'))
    const ran = registerHooks(session)

    assert.doesNotThrow(() => session.close())

    assert.deepStrictEqual(ran, ['first', 'throws', 'last'])
    assert.ok(session.signal.aborted)
    assert.strictEqual(session.pubsubSubscriptionCount, 0)
    assert.strictEqual(session.mode, 'normal')
    assert.strictEqual(session.isWatchDirty(), false)

    const reader = new AbortController()
    const pushes = session.readPushes(reader.signal)[Symbol.asyncIterator]()
    assert.deepStrictEqual(await pushes.next(), {
      done: true,
      value: undefined,
    })
  })

  test('RESET runs every hook, finishes the reset and replies +RESET', async () => {
    const { session } = createSession()
    await session.execute('hello', buffers('3'))
    await session.execute('select', buffers('1'))
    await session.execute('subscribe', buffers('a'))
    const ran = registerHooks(session)

    const reply = await session.execute('reset', [])

    assert.deepStrictEqual(reply.value, {
      kind: 'simple-string',
      value: 'RESET',
    })
    assert.deepStrictEqual(ran, ['first', 'throws', 'last'])
    assert.strictEqual(session.pubsubSubscriptionCount, 0)
    assert.strictEqual(session.mode, 'normal')
    assert.strictEqual(session.selectedDatabase, 0)
    assert.strictEqual(session.protocolVersion, 2)

    // Each hook ran once: a second RESET finds none left.
    await session.execute('reset', [])
    assert.deepStrictEqual(ran, ['first', 'throws', 'last'])
  })

  test('QUIT inside MULTI is answered with close and runs nothing queued', async () => {
    const { server, session } = createSession()
    await session.execute('multi', [])
    await session.execute('set', buffers('quit-key', 'v'))

    const reply = await session.execute('quit', [])

    assert.deepStrictEqual(reply.value, { kind: 'simple-string', value: 'OK' })
    assert.strictEqual(reply.options?.close, true)
    session.close()
    assert.strictEqual(session.mode, 'normal')
    assert.strictEqual(server.getDatabase(0).get(Buffer.from('quit-key')), null)
  })
})
