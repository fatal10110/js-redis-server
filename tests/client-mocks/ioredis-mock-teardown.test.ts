import { describe, test } from 'node:test'
import assert from 'node:assert'
import type { Redis } from 'ioredis'
import { createIoredisMock } from '../../src/index'
import { createRedisCommandExecutor } from '../../src/commands'
import { RedisServerState } from '../../src/state'
import { createVirtualConnection } from '../../src/core/transports/virtual-connection'

describe('createIoredisMock — teardown', () => {
  test('quit() ends the session and rejects later commands', async () => {
    const redis = (await createIoredisMock()) as Redis
    await redis.set('k', 'v')

    const ended = once(redis, 'end')
    assert.strictEqual(await redis.quit(), 'OK')
    await ended

    assert.strictEqual(redis.status, 'end')
    await assert.rejects(redis.get('k'))
  })

  test('disconnect() ends the session and rejects later commands', async () => {
    const redis = (await createIoredisMock()) as Redis
    await redis.set('k', 'v')

    const ended = once(redis, 'end')
    redis.disconnect()
    await ended

    assert.strictEqual(redis.status, 'end')
    await assert.rejects(redis.get('k'))
  })

  test('a reply far larger than the stream high-water mark round-trips', async () => {
    // The virtual wire is a duplexPair, so a reply bigger than the default
    // 16 KiB high-water mark must not wedge the server's write loop.
    const redis = (await createIoredisMock()) as Redis
    const value = 'x'.repeat(1024 * 1024)

    await redis.set('big', value)
    assert.strictEqual(await redis.get('big'), value)

    await redis.quit()
  })
})

describe('virtual connection teardown symmetry', () => {
  test('server close() and client destroy() both end the session', async () => {
    for (const teardown of ['server', 'client'] as const) {
      const state = new RedisServerState({ databaseCount: 16 })
      const executor = createRedisCommandExecutor()
      const connection = createVirtualConnection({ state, executor })

      await once(connection.clientSocket, 'connect')
      assert.strictEqual(state.getConnectedClients().length, 1)

      if (teardown === 'server') {
        connection.close()
      } else {
        connection.clientSocket.destroy()
      }

      await connection.done
      assert.strictEqual(
        state.getConnectedClients().length,
        0,
        `${teardown}-initiated teardown left the session open`,
      )
      state.close()
    }
  })
})

function once(emitter: NodeJS.EventEmitter, event: string): Promise<void> {
  return new Promise(resolve => emitter.once(event, () => resolve()))
}
