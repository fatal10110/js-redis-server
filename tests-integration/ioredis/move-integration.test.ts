import { Redis, type Cluster } from 'ioredis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, randomKey } from '../utils'
import { errorWithMessage } from '../../tests/shared-test-helpers'

const testRunner = new TestRunner()

describe('MOVE command integration (standalone)', () => {
  let client: Redis | undefined

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('moves a key to another database and removes it from the source', async () => {
    const key = randomKey()

    await client!.select(0)
    await client!.set(key, 'value')

    assert.strictEqual(await client!.move(key, '1'), 1)
    assert.strictEqual(await client!.get(key), null)

    await client!.select(1)
    assert.strictEqual(await client!.get(key), 'value')
    await client!.select(0)
  })

  test('returns 0 when the source key is missing', async () => {
    const key = randomKey()

    await client!.select(0)
    assert.strictEqual(await client!.move(key, '1'), 0)

    await client!.select(1)
    assert.strictEqual(await client!.exists(key), 0)
    await client!.select(0)
  })

  test('returns 0 and leaves both databases unchanged when destination exists', async () => {
    const key = randomKey()

    await client!.select(0)
    await client!.set(key, 'source')
    await client!.select(1)
    await client!.set(key, 'destination')

    await client!.select(0)
    assert.strictEqual(await client!.move(key, '1'), 0)
    assert.strictEqual(await client!.get(key), 'source')

    await client!.select(1)
    assert.strictEqual(await client!.get(key), 'destination')
    await client!.select(0)
  })

  test('preserves TTL on the moved key', async () => {
    const key = randomKey()

    await client!.select(0)
    await client!.set(key, 'value', 'EX', 1000)

    assert.strictEqual(await client!.move(key, '1'), 1)

    await client!.select(1)
    const ttl = await client!.ttl(key)
    assert.ok(ttl > 990 && ttl <= 1000, `expected ttl ~1000, got ${ttl}`)
    await client!.select(0)
  })
})

describe(`MOVE command cluster rejection (${testRunner.getBackendName()})`, () => {
  let cluster: Cluster | undefined

  before(async () => {
    cluster = await testRunner.setupIoredisCluster('move-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('rejects MOVE in cluster mode', async () => {
    const key = `{move:${randomKey()}}:cluster`
    const slotOwner = await connectToSlotOwner(cluster!, key)

    try {
      await assert.rejects(
        slotOwner.move(key, '1'),
        errorWithMessage('ERR MOVE is not allowed in cluster mode'),
      )
    } finally {
      slotOwner.disconnect()
    }
  })
})
