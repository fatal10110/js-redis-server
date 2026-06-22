import { Cluster, Redis } from 'ioredis'
import { after, before, describe, it } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage } from '../utils'

const testRunner = new TestRunner()

// Real Redis sets CLIENT_DIRTY_EXEC when a queued command fails (e.g. an
// unknown command) and CLIENT_DIRTY_CAS when a WATCHed key is touched. EXEC
// must prioritise DIRTY_EXEC: if the queue itself is bad it returns
// -EXECABORT regardless of WATCH state, only returning the (nil) CAS-abort
// reply when the queue is clean. See issue #123.
describe('EXEC dirty-flag precedence (EXECABORT over WATCH-nil)', () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  it('returns EXECABORT (not nil) when the queue is bad AND a watched key changed', async () => {
    const key = 'execdirty:precedence:1'
    let directClient: Redis | undefined
    let mutatingClient: Redis | undefined

    try {
      // Two direct connections to the same slot owner so WATCH/MULTI/EXEC stay
      // on one node and the mutation is seen by the watching session.
      directClient = await connectToSlotOwner(redisClient!, key)
      mutatingClient = await connectToSlotOwner(redisClient!, key)

      await directClient.set(key, 'init')
      await directClient.call('WATCH', key)

      // Dirty the WATCH (CLIENT_DIRTY_CAS) from another connection.
      await mutatingClient.set(key, 'modified')

      assert.strictEqual(await directClient.call('MULTI'), 'OK')

      // Queue an unknown command -> dirties the transaction (CLIENT_DIRTY_EXEC).
      await assert.rejects(() => directClient!.call('NOTACOMMAND', 'x'))

      // DIRTY_EXEC must win over DIRTY_CAS: EXECABORT, not a nil array.
      await assert.rejects(
        () => directClient!.call('EXEC'),
        errorWithMessage(
          'EXECABORT Transaction discarded because of previous errors.',
        ),
      )
    } finally {
      directClient?.disconnect()
      mutatingClient?.disconnect()
    }
  })

  it('returns EXECABORT when the queue is bad and no key is watched', async () => {
    const key = 'execdirty:precedence:2'
    let directClient: Redis | undefined

    try {
      directClient = await connectToSlotOwner(redisClient!, key)

      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      await assert.rejects(() => directClient!.call('NOTACOMMAND', 'x'))

      await assert.rejects(
        () => directClient!.call('EXEC'),
        errorWithMessage(
          'EXECABORT Transaction discarded because of previous errors.',
        ),
      )
    } finally {
      directClient?.disconnect()
    }
  })

  it('returns nil (not EXECABORT) when only a watched key changed and the queue is clean', async () => {
    const key = 'execdirty:precedence:3'
    let directClient: Redis | undefined
    let mutatingClient: Redis | undefined

    try {
      directClient = await connectToSlotOwner(redisClient!, key)
      mutatingClient = await connectToSlotOwner(redisClient!, key)

      await directClient.set(key, 'init')
      await directClient.call('WATCH', key)
      await mutatingClient.set(key, 'modified')

      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(await directClient.set(key, 'queued'), 'QUEUED')

      // Clean queue + dirty CAS -> aborts with a nil array, never EXECABORT.
      const result = await directClient.call('EXEC')
      assert.strictEqual(result, null)

      // The watched key keeps the other client's value.
      assert.strictEqual(await directClient.get(key), 'modified')
    } finally {
      directClient?.disconnect()
      mutatingClient?.disconnect()
    }
  })

  it('returns ERR EXEC without MULTI when called outside a transaction', async () => {
    const key = 'execdirty:precedence:4'
    let directClient: Redis | undefined

    try {
      directClient = await connectToSlotOwner(redisClient!, key)
      await assert.rejects(
        () => directClient!.call('EXEC'),
        errorWithMessage('ERR EXEC without MULTI'),
      )
    } finally {
      directClient?.disconnect()
    }
  })
})
