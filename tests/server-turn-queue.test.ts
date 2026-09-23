import { describe, test } from 'node:test'
import assert from 'node:assert'
import { ClientSession, RedisResult, RedisValue } from '../src/internal'
import { createRedisSessionHarness as createHarness } from './core-session-test-helpers'

// One SerialTurnQueue per RedisServerState (#369): real Redis is
// single-threaded across every database, so sessions on different DBs of the
// same server serialize against each other, and a blocking command parked on
// one DB still yields that single turn to sessions on any other DB.

function buf(...tokens: string[]): Buffer[] {
  return tokens.map(t => Buffer.from(t))
}

function arrayResult(items: string[]): RedisResult {
  return RedisResult.create(
    RedisValue.array(items.map(s => RedisValue.bulkString(Buffer.from(s)))),
  )
}

function yieldToEventLoop(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve))
}

describe('server-wide turn queue (#369)', () => {
  test('sessions on different databases share one turn', async () => {
    const { server, executor } = createHarness({ databaseCount: 2 })
    const db0 = new ClientSession({ server, executor, database: 0 })
    const db1 = new ClientSession({ server, executor, database: 1 })

    // Hold the server's only turn; a command on *either* database must wait.
    const blocker = await server.turnQueue.waitTurn()

    const settled: string[] = []
    const onDb0 = db0.execute('set', buf('k', 'zero')).then(result => {
      settled.push('db0')
      return result
    })
    const onDb1 = db1.execute('set', buf('k', 'one')).then(result => {
      settled.push('db1')
      return result
    })

    await new Promise(resolve => setTimeout(resolve, 20))
    assert.deepStrictEqual(settled, [], 'both DBs wait on the same turn')
    assert.strictEqual(server.getDatabase(0).getString(Buffer.from('k')), null)
    assert.strictEqual(server.getDatabase(1).getString(Buffer.from('k')), null)

    blocker.release()
    assert.deepStrictEqual(await onDb0, RedisResult.ok())
    assert.deepStrictEqual(await onDb1, RedisResult.ok())
    // FIFO across databases: DB 0's command was queued first.
    assert.deepStrictEqual(settled, ['db0', 'db1'])
    assert.deepStrictEqual(
      server.getDatabase(1).getString(Buffer.from('k')),
      Buffer.from('one'),
    )
  })

  test('blocking commands on different databases park and resume independently', async () => {
    const { server, executor } = createHarness({ databaseCount: 2 })
    const waiter0 = new ClientSession({ server, executor, database: 0 })
    const waiter1 = new ClientSession({ server, executor, database: 1 })
    const pusher0 = new ClientSession({ server, executor, database: 0 })
    const pusher1 = new ClientSession({ server, executor, database: 1 })

    // Same key name on both DBs: each waiter must only see its own DB's push.
    const blocked0 = waiter0.execute('blpop', buf('q', '5'))
    const blocked1 = waiter1.execute('blpop', buf('q', '5'))
    await yieldToEventLoop()

    // Both waiters are parked, so the single server turn is free for others.
    assert.deepStrictEqual(
      await pusher1.execute('rpush', buf('q', 'from-db1')),
      RedisResult.create(RedisValue.integer(1)),
    )
    assert.deepStrictEqual(await blocked1, arrayResult(['q', 'from-db1']))

    let db0Settled = false
    void blocked0.then(() => {
      db0Settled = true
    })
    await yieldToEventLoop()
    assert.strictEqual(db0Settled, false, 'DB 1 push must not wake DB 0 waiter')

    assert.deepStrictEqual(
      await pusher0.execute('rpush', buf('q', 'from-db0')),
      RedisResult.create(RedisValue.integer(1)),
    )
    assert.deepStrictEqual(await blocked0, arrayResult(['q', 'from-db0']))

    // Both lists were popped empty and deleted.
    assert.strictEqual(server.getDatabase(0).get(Buffer.from('q')), null)
    assert.strictEqual(server.getDatabase(1).get(Buffer.from('q')), null)
  })
})
