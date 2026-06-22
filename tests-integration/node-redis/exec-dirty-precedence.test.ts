import { RedisClientType, RedisClusterType } from 'redis'
import { after, before, describe, it } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { connectToNodeRedisSlotOwner, errorWithMessage } from '../utils'

const testRunner = new TestRunner()

// Real Redis sets CLIENT_DIRTY_EXEC when a queued command fails (e.g. an
// unknown command) and CLIENT_DIRTY_CAS when a WATCHed key is touched. EXEC
// must prioritise DIRTY_EXEC: if the queue itself is bad it returns
// -EXECABORT regardless of WATCH state, only returning the (nil) CAS-abort
// reply when the queue is clean. See issue #123.
//
// node-redis' typed multi() can't queue an unknown command, so these drive the
// raw MULTI/EXEC protocol over direct slot-owner connections.
describe('EXEC dirty-flag precedence (EXECABORT over WATCH-nil) (node-redis)', () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  it('returns EXECABORT (not nil) when the queue is bad AND a watched key changed', async () => {
    const key = 'execdirty:precedence:1'
    let directClient: RedisClientType | undefined
    let mutatingClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      mutatingClient = await connectToNodeRedisSlotOwner(redisClient, key)

      await directClient.sendCommand(['SET', key, 'init'])
      await directClient.sendCommand(['WATCH', key])

      await mutatingClient.sendCommand(['SET', key, 'modified'])

      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')

      await assert.rejects(() =>
        directClient!.sendCommand(['NOTACOMMAND', 'x']),
      )

      await assert.rejects(
        () => directClient!.sendCommand(['EXEC']),
        errorWithMessage(
          'EXECABORT Transaction discarded because of previous errors.',
        ),
      )
    } finally {
      directClient?.destroy()
      mutatingClient?.destroy()
    }
  })

  it('returns EXECABORT when the queue is bad and no key is watched', async () => {
    const key = 'execdirty:precedence:2'
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)

      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      await assert.rejects(() =>
        directClient!.sendCommand(['NOTACOMMAND', 'x']),
      )

      await assert.rejects(
        () => directClient!.sendCommand(['EXEC']),
        errorWithMessage(
          'EXECABORT Transaction discarded because of previous errors.',
        ),
      )
    } finally {
      directClient?.destroy()
    }
  })

  it('returns nil (not EXECABORT) when only a watched key changed and the queue is clean', async () => {
    const key = 'execdirty:precedence:3'
    let directClient: RedisClientType | undefined
    let mutatingClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      mutatingClient = await connectToNodeRedisSlotOwner(redisClient, key)

      await directClient.sendCommand(['SET', key, 'init'])
      await directClient.sendCommand(['WATCH', key])
      await mutatingClient.sendCommand(['SET', key, 'modified'])

      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(
        await directClient.sendCommand(['SET', key, 'queued']),
        'QUEUED',
      )

      const result = await directClient.sendCommand(['EXEC'])
      assert.strictEqual(result, null)

      assert.strictEqual(
        await directClient.sendCommand(['GET', key]),
        'modified',
      )
    } finally {
      directClient?.destroy()
      mutatingClient?.destroy()
    }
  })

  it('returns ERR EXEC without MULTI when called outside a transaction', async () => {
    const key = 'execdirty:precedence:4'
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await assert.rejects(
        () => directClient!.sendCommand(['EXEC']),
        errorWithMessage('ERR EXEC without MULTI'),
      )
    } finally {
      directClient?.destroy()
    }
  })
})
