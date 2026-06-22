import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { errorWithMessage } from '../../tests/shared-test-helpers'

const testRunner = new TestRunner()

describe(`COPY command integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('copy-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('copies a value to a new destination and returns 1', async () => {
    const tag = `{copy:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await redisClient!.set(src, 'hello')

    assert.strictEqual(await redisClient!.copy(src, dst), 1)
    assert.strictEqual(await redisClient!.get(dst), 'hello')
    // source is left intact
    assert.strictEqual(await redisClient!.get(src), 'hello')
  })

  test('returns 0 when destination already exists without REPLACE', async () => {
    const tag = `{copy:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await redisClient!.set(src, 'hello')
    await redisClient!.set(dst, 'existing')

    assert.strictEqual(await redisClient!.copy(src, dst), 0)
    // destination is untouched
    assert.strictEqual(await redisClient!.get(dst), 'existing')
  })

  test('overwrites an existing destination with REPLACE and returns 1', async () => {
    const tag = `{copy:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await redisClient!.set(src, 'hello')
    await redisClient!.set(dst, 'existing')

    assert.strictEqual(await redisClient!.copy(src, dst, 'REPLACE'), 1)
    assert.strictEqual(await redisClient!.get(dst), 'hello')
  })

  test('returns 0 when the source key does not exist', async () => {
    const tag = `{copy:${randomKey()}}`
    const src = `${tag}:missing`
    const dst = `${tag}:dst`

    assert.strictEqual(await redisClient!.copy(src, dst), 0)
    assert.strictEqual(await redisClient!.exists(dst), 0)
  })

  test('errors when source and destination are the same key', async () => {
    const tag = `{copy:${randomKey()}}`
    const key = `${tag}:same`

    await redisClient!.set(key, 'hello')

    await assert.rejects(
      redisClient!.copy(key, key),
      errorWithMessage('ERR source and destination objects are the same'),
    )
  })

  test('copies the TTL together with the value', async () => {
    const tag = `{copy:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await redisClient!.set(src, 'hello', 'EX', 1000)

    assert.strictEqual(await redisClient!.copy(src, dst), 1)
    const ttl = await redisClient!.ttl(dst)
    assert.ok(ttl > 990 && ttl <= 1000, `expected ttl ~1000, got ${ttl}`)
  })

  test('REPLACE overwrites the destination TTL with the source TTL (none)', async () => {
    const tag = `{copy:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await redisClient!.set(src, 'hello')
    await redisClient!.set(dst, 'existing', 'EX', 1000)

    assert.strictEqual(await redisClient!.copy(src, dst, 'REPLACE'), 1)
    // source had no TTL, so destination must end up persistent (-1)
    assert.strictEqual(await redisClient!.ttl(dst), -1)
  })

  test('works on any value type (list)', async () => {
    const tag = `{copy:${randomKey()}}`
    const src = `${tag}:src`
    const dst = `${tag}:dst`

    await redisClient!.rpush(src, 'a', 'b', 'c')

    assert.strictEqual(await redisClient!.copy(src, dst), 1)
    assert.deepStrictEqual(await redisClient!.lrange(dst, 0, -1), [
      'a',
      'b',
      'c',
    ])
  })

  test('rejects keys that hash to different slots with CROSSSLOT', async () => {
    const src = `{copy-a:${randomKey()}}:src`
    const dst = `{copy-b:${randomKey()}}:dst`

    await redisClient!.set(src, 'hello')

    await assert.rejects(
      redisClient!.copy(src, dst),
      errorWithMessage("CROSSSLOT Keys in request don't hash to the same slot"),
    )
  })
})

// DB option and SELECT-style multi-database behavior cannot be exercised in
// cluster mode (only DB 0 exists), so the DB tests run against a standalone
// server. Argument-validation errors are slot-independent and also live here.
describe('COPY command DB option and errors (standalone)', () => {
  let client: Redis | undefined

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('copies across databases with the DB option', async () => {
    const key = randomKey()

    await client!.select(0)
    await client!.set(key, 'hello')

    assert.strictEqual(await client!.copy(key, key, 'DB', 1), 1)

    await client!.select(1)
    assert.strictEqual(await client!.get(key), 'hello')

    await client!.select(0)
  })

  test('same key name in a different DB is allowed (not "same objects")', async () => {
    const key = randomKey()

    await client!.select(0)
    await client!.set(key, 'value')

    // same key name but DB differs -> allowed
    assert.strictEqual(await client!.copy(key, key, 'DB', 1, 'REPLACE'), 1)

    await client!.select(0)
  })
})
