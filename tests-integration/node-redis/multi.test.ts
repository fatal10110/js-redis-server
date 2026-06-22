import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import assert from 'node:assert'
import { MultiErrorReply, RedisClusterType } from 'redis'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe('multi (node-redis)', () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('Queue commands before execution without piplining', async () => {
    const anotherRedisClient =
      (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    const key = `{node-redis-multi:${randomKey()}}:queued`

    try {
      const multi = redisClient.multi()
      multi.set(key, 'myValue')
      const anotherRes = await anotherRedisClient.get(key)
      multi.get(key)

      const res = await multi.exec()

      assert.notEqual(res, null)
      const [first, second] = res
      assert.strictEqual(first, 'OK')
      assert.strictEqual(second, 'myValue')
      assert.strictEqual(anotherRes, null)
    } finally {
      await redisClient.del(key)
      await anotherRedisClient.close()
    }
  })

  test('aborts the whole transaction (EXECABORT) when a command fails to queue', async () => {
    // node-redis' typed multi() can't queue an arity-invalid command and
    // crashes on the EXECABORT reply, so drive the raw protocol instead.
    const key = `{node-redis-multi:${randomKey()}}:execabort`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(
        await directClient.sendCommand(['SET', key, 'myValue']),
        'QUEUED',
      )
      // EVALSHA with no numkeys fails at queue time -> CLIENT_DIRTY_EXEC.
      await assert.rejects(() => directClient.sendCommand(['EVALSHA', 'abc']))

      await assert.rejects(
        () => directClient.sendCommand(['EXEC']),
        errorWithMessage(
          'EXECABORT Transaction discarded because of previous errors.',
        ),
      )
      // The queued SET must not have run.
      assert.strictEqual(await directClient.get(key), null)
    } finally {
      directClient.destroy()
    }
  })

  test('returns execution errors from EXEC replies', async () => {
    const key = `{node-redis-multi:${randomKey()}}:error`
    let error: MultiErrorReply | undefined
    try {
      const multi = redisClient.multi()
      multi.set(key, 'myValue')
      multi.evalSha('abc')

      try {
        await multi.exec()
      } catch (err) {
        assert.ok(err instanceof MultiErrorReply)
        error = err
      }

      assert.ok(error)
      assert.deepStrictEqual(error.errorIndexes, [1])
      assert.strictEqual(error.replies[0], 'OK')
      assert.ok(error.replies[1] instanceof Error)
      assert.match(error.replies[1].message, /NOSCRIPT/)
    } finally {
      await redisClient.del(key)
    }
  })

  test('runtime command errors are surfaced via MultiErrorReply', async () => {
    const key = `{tx-wrong-type:${randomKey()}}:list`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.lPush(key, 'value')

      let error: MultiErrorReply | undefined
      try {
        await directClient.multi().get(key).exec()
      } catch (err) {
        assert.ok(err instanceof MultiErrorReply)
        error = err
      }

      assert.ok(error)
      assert.deepStrictEqual(error.errorIndexes, [0])
      assert.ok(error.replies[0] instanceof Error)
      assert.strictEqual(
        error.replies[0].message,
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      )
    } finally {
      await directClient.del(key)
      directClient.destroy()
    }
  })

  test('DISCARD clears queued writes without executing them', async () => {
    const key = `{discard:${randomKey()}}:key`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(
        await directClient.sendCommand(['SET', key, 'value']),
        'QUEUED',
      )
      assert.strictEqual(await directClient.sendCommand(['DISCARD']), 'OK')
      assert.strictEqual(await directClient.get(key), null)
    } finally {
      directClient.destroy()
    }
  })

  test('transaction mode errors match Redis', async () => {
    const key = `{tx-errors:${randomKey()}}:key`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await assert.rejects(
        () => directClient.sendCommand(['EXEC']),
        errorWithMessage('ERR EXEC without MULTI'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['DISCARD']),
        errorWithMessage('ERR DISCARD without MULTI'),
      )

      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      await assert.rejects(
        () => directClient.sendCommand(['WATCH', key]),
        errorWithMessage('ERR WATCH inside MULTI is not allowed'),
      )
      assert.deepStrictEqual(await directClient.sendCommand(['EXEC']), [])
    } finally {
      directClient.destroy()
    }
  })

  test('EXEC with wrong arity aborts the transaction with EXECABORT', async () => {
    const key = `{exec-arity:${randomKey()}}:key`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(
        await directClient.sendCommand(['SET', key, 'value']),
        'QUEUED',
      )
      await assert.rejects(
        () => directClient.sendCommand(['EXEC', 'foo']),
        errorWithMessage(
          "EXECABORT Transaction discarded because of: wrong number of arguments for 'exec' command",
        ),
      )

      // Session must be back in normal mode: this SET runs immediately.
      assert.strictEqual(
        await directClient.sendCommand(['SET', key, 'value2']),
        'OK',
      )
      assert.strictEqual(await directClient.get(key), 'value2')
    } finally {
      await directClient.del(key)
      directClient.destroy()
    }
  })

  test('UNWATCH is queued in MULTI and returns OK from EXEC', async () => {
    const key = `{unwatch:${randomKey()}}:key`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      assert.strictEqual(await directClient.sendCommand(['WATCH', key]), 'OK')
      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(await directClient.sendCommand(['UNWATCH']), 'QUEUED')
      assert.deepStrictEqual(await directClient.sendCommand(['EXEC']), ['OK'])
    } finally {
      directClient.destroy()
    }
  })
})
