import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()
// Unique per run: the real-backend suites share one Redis that is never
// flushed between files or between runs, so fixed literal key names collided
// with each other and with their own previous run (#420).
const RUN = randomKey()

describe(`String Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('string-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('APPEND command', async () => {
    // APPEND to non-existent key
    const append1 = await redisClient?.append(`appendkey:${RUN}`, 'hello')
    assert.strictEqual(append1, 5)

    // APPEND to existing key
    const append2 = await redisClient?.append(`appendkey:${RUN}`, ' world')
    assert.strictEqual(append2, 11)

    const value = await redisClient?.get(`appendkey:${RUN}`)
    assert.strictEqual(value, 'hello world')
  })

  test('STRLEN command', async () => {
    // STRLEN on non-existent key
    const len1 = await redisClient?.strlen(`nonexistent:${RUN}`)
    assert.strictEqual(len1, 0)

    await redisClient?.set(`strlenkey:${RUN}`, 'hello')
    const len2 = await redisClient?.strlen(`strlenkey:${RUN}`)
    assert.strictEqual(len2, 5)
  })

  test('MGET command', async () => {
    await redisClient?.set(`{same:${RUN}}mget1`, 'value1')
    await redisClient?.set(`{same:${RUN}}mget2`, 'value2')

    const values = await redisClient?.mget(
      `{same:${RUN}}mget1`,
      `{same:${RUN}}mget2`,
      `{same:${RUN}}nonexistent`,
    )
    assert.deepStrictEqual(values, ['value1', 'value2', null])
  })

  test('MGET cross-slot error', async () => {
    await redisClient?.set('{mget-slot-a}key', 'value1')
    await redisClient?.set('{mget-slot-b}key', 'value2')

    await assert.rejects(
      () => redisClient?.mget('{mget-slot-a}key', '{mget-slot-b}key'),
      errorWithMessage("CROSSSLOT Keys in request don't hash to the same slot"),
    )
  })

  test('MSET command', async () => {
    await redisClient?.mset(
      `{same:${RUN}}mset1`,
      'value1',
      `{same:${RUN}}mset2`,
      'value2',
      `{same:${RUN}}mset3`,
      'value3',
    )

    const get1 = await redisClient?.get(`{same:${RUN}}mset1`)
    const get2 = await redisClient?.get(`{same:${RUN}}mset2`)
    const get3 = await redisClient?.get(`{same:${RUN}}mset3`)

    assert.strictEqual(get1, 'value1')
    assert.strictEqual(get2, 'value2')
    assert.strictEqual(get3, 'value3')
  })

  test('MSETNX command', async () => {
    // All keys new
    const result1 = await redisClient?.msetnx(
      `{same:${RUN}}msetnx1`,
      'value1',
      `{same:${RUN}}msetnx2`,
      'value2',
    )
    assert.strictEqual(result1, 1)

    // Some keys exist
    const result2 = await redisClient?.msetnx(
      `{same:${RUN}}msetnx1`,
      'newvalue',
      `{same:${RUN}}msetnx3`,
      'value3',
    )
    assert.strictEqual(result2, 0)

    // Verify original values unchanged
    const check = await redisClient?.get(`{same:${RUN}}msetnx1`)
    assert.strictEqual(check, 'value1')
  })

  test('GETSET command', async () => {
    await redisClient?.set(`getsetkey:${RUN}`, 'oldvalue')

    const oldValue = await redisClient?.getset(`getsetkey:${RUN}`, 'newvalue')
    assert.strictEqual(oldValue, 'oldvalue')

    const newValue = await redisClient?.get(`getsetkey:${RUN}`)
    assert.strictEqual(newValue, 'newvalue')

    // GETSET on non-existent key
    const nullValue = await redisClient?.getset(
      `newgetsetkey:${RUN}`,
      'firstvalue',
    )
    assert.strictEqual(nullValue, null)
  })

  test('SUBSTR aliases GETRANGE', async () => {
    const tag = `{substr:${randomKey()}}`
    const key = `${tag}:key`
    const missingKey = `${tag}:missing`
    const directClient = await connectToSlotOwner(redisClient!, key)

    try {
      await directClient.set(key, 'abcdef')

      assert.strictEqual(await directClient.substr(key, '1', '3'), 'bcd')
      assert.strictEqual(await directClient.substr(key, '-3', '-1'), 'def')
      assert.strictEqual(await directClient.substr(missingKey, '0', '1'), '')
    } finally {
      await directClient.del(key, missingKey)
      directClient.disconnect()
    }
  })

  test('String commands workflow', async () => {
    // Create a session counter with user data
    await redisClient?.set(`{user1001:${RUN}}name`, 'Alice')
    await redisClient?.set(`{user1001:${RUN}}sessions`, '0')

    // Increment session count
    const sessions1 = await redisClient?.incr(`{user1001:${RUN}}sessions`)
    assert.strictEqual(sessions1, 1)

    // Add login timestamp
    await redisClient?.append(`{user1001:${RUN}}name`, ' (Online)')
    const nameWithStatus = await redisClient?.get(`{user1001:${RUN}}name`)
    assert.strictEqual(nameWithStatus, 'Alice (Online)')

    // Get multiple user fields
    const userData = await redisClient?.mget(
      `{user1001:${RUN}}name`,
      `{user1001:${RUN}}sessions`,
    )
    assert.deepStrictEqual(userData, ['Alice (Online)', '1'])

    // Update multiple fields atomically
    await redisClient?.mset(
      `{user1001:${RUN}}lastlogin`,
      Date.now().toString(),
      `{user1001:${RUN}}score`,
      '0',
    )

    // Increment score by points
    await redisClient?.incrby(`{user1001:${RUN}}score`, 150)
    const score = await redisClient?.get(`{user1001:${RUN}}score`)
    assert.strictEqual(score, '150')

    // Check total data length
    const nameLen = await redisClient?.strlen(`{user1001:${RUN}}name`)
    assert.strictEqual(nameLen, 14) // 'Alice (Online)'.length
  })

  test('String numeric and expiration errors match Redis', async () => {
    const tag = `{string-errors:${randomKey()}}`
    const stringKey = `${tag}:string`
    const leadingZeroKey = `${tag}:leading-zero`
    const negativeLeadingZeroKey = `${tag}:negative-leading-zero`
    const zeroKey = `${tag}:zero`
    const negativeZeroKey = `${tag}:negative-zero`
    const directClient = await connectToSlotOwner(redisClient!, stringKey)

    try {
      await directClient.set(stringKey, 'not-a-number')
      await directClient.set(leadingZeroKey, '007')
      await directClient.set(negativeLeadingZeroKey, '-01')
      await directClient.set(zeroKey, '0')
      await directClient.set(negativeZeroKey, '-0')

      await assert.rejects(
        () => directClient.incr(stringKey),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.incr(leadingZeroKey),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.decr(negativeLeadingZeroKey),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      assert.strictEqual(await directClient.incr(zeroKey), 1)
      await assert.rejects(
        () => directClient.incr(negativeZeroKey),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.incrby(stringKey, 'abc'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.incrby(stringKey, '01'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.decrby(stringKey, '-01'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.incrbyfloat(stringKey, 'abc'),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => directClient.setrange(stringKey, '-1', 'x'),
        errorWithMessage('ERR offset is out of range'),
      )
      await assert.rejects(
        () => directClient.setex(`${tag}:setex`, '0', 'value'),
        errorWithMessage("ERR invalid expire time in 'setex' command"),
      )
      await assert.rejects(
        () => directClient.setex(`${tag}:setex-leading-zero`, '01', 'value'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.psetex(`${tag}:psetex`, '0', 'value'),
        errorWithMessage("ERR invalid expire time in 'psetex' command"),
      )
      await assert.rejects(
        () => directClient.getex(stringKey, 'EX', '0'),
        errorWithMessage("ERR invalid expire time in 'getex' command"),
      )
      await assert.rejects(
        () => directClient.getex(stringKey, 'EX', '10', 'PX', '10'),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await directClient.del(
        stringKey,
        leadingZeroKey,
        negativeLeadingZeroKey,
        zeroKey,
        negativeZeroKey,
        `${tag}:setex`,
        `${tag}:setex-leading-zero`,
        `${tag}:psetex`,
      )
      directClient.disconnect()
    }
  })

  test('MGET returns null for keys holding non-string values', async () => {
    const tag = `{mget-types:${randomKey()}}`
    const stringKey = `${tag}:string`
    const listKey = `${tag}:list`
    const missingKey = `${tag}:missing`

    await redisClient?.set(stringKey, 'A')
    await redisClient?.lpush(listKey, 'B')

    assert.deepStrictEqual(
      await redisClient?.mget(stringKey, listKey, missingKey),
      ['A', null, null],
    )
  })
})
