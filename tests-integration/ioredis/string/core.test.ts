import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

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
    const append1 = await redisClient?.append('appendkey', 'hello')
    assert.strictEqual(append1, 5)

    // APPEND to existing key
    const append2 = await redisClient?.append('appendkey', ' world')
    assert.strictEqual(append2, 11)

    const value = await redisClient?.get('appendkey')
    assert.strictEqual(value, 'hello world')
  })

  test('STRLEN command', async () => {
    // STRLEN on non-existent key
    const len1 = await redisClient?.strlen('nonexistent')
    assert.strictEqual(len1, 0)

    await redisClient?.set('strlenkey', 'hello')
    const len2 = await redisClient?.strlen('strlenkey')
    assert.strictEqual(len2, 5)
  })

  test('MGET command', async () => {
    await redisClient?.set('{same}mget1', 'value1')
    await redisClient?.set('{same}mget2', 'value2')

    const values = await redisClient?.mget(
      '{same}mget1',
      '{same}mget2',
      '{same}nonexistent',
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
      '{same}mset1',
      'value1',
      '{same}mset2',
      'value2',
      '{same}mset3',
      'value3',
    )

    const get1 = await redisClient?.get('{same}mset1')
    const get2 = await redisClient?.get('{same}mset2')
    const get3 = await redisClient?.get('{same}mset3')

    assert.strictEqual(get1, 'value1')
    assert.strictEqual(get2, 'value2')
    assert.strictEqual(get3, 'value3')
  })

  test('MSETNX command', async () => {
    // All keys new
    const result1 = await redisClient?.msetnx(
      '{same}msetnx1',
      'value1',
      '{same}msetnx2',
      'value2',
    )
    assert.strictEqual(result1, 1)

    // Some keys exist
    const result2 = await redisClient?.msetnx(
      '{same}msetnx1',
      'newvalue',
      '{same}msetnx3',
      'value3',
    )
    assert.strictEqual(result2, 0)

    // Verify original values unchanged
    const check = await redisClient?.get('{same}msetnx1')
    assert.strictEqual(check, 'value1')
  })

  test('GETSET command', async () => {
    await redisClient?.set('getsetkey', 'oldvalue')

    const oldValue = await redisClient?.getset('getsetkey', 'newvalue')
    assert.strictEqual(oldValue, 'oldvalue')

    const newValue = await redisClient?.get('getsetkey')
    assert.strictEqual(newValue, 'newvalue')

    // GETSET on non-existent key
    const nullValue = await redisClient?.getset('newgetsetkey', 'firstvalue')
    assert.strictEqual(nullValue, null)
  })

  test('SUBSTR aliases GETRANGE', async () => {
    const tag = `{substr:${randomKey()}}`
    const key = `${tag}:key`
    const missingKey = `${tag}:missing`
    const directClient = await connectToSlotOwner(redisClient!, key)

    try {
      await directClient.set(key, 'abcdef')

      assert.strictEqual(
        await directClient.call('SUBSTR', key, '1', '3'),
        'bcd',
      )
      assert.strictEqual(
        await directClient.call('SUBSTR', key, '-3', '-1'),
        'def',
      )
      assert.strictEqual(
        await directClient.call('SUBSTR', missingKey, '0', '1'),
        '',
      )
    } finally {
      await directClient.del(key, missingKey)
      directClient.disconnect()
    }
  })

  test('String commands workflow', async () => {
    // Create a session counter with user data
    await redisClient?.set('{user1001}name', 'Alice')
    await redisClient?.set('{user1001}sessions', '0')

    // Increment session count
    const sessions1 = await redisClient?.incr('{user1001}sessions')
    assert.strictEqual(sessions1, 1)

    // Add login timestamp
    await redisClient?.append('{user1001}name', ' (Online)')
    const nameWithStatus = await redisClient?.get('{user1001}name')
    assert.strictEqual(nameWithStatus, 'Alice (Online)')

    // Get multiple user fields
    const userData = await redisClient?.mget(
      '{user1001}name',
      '{user1001}sessions',
    )
    assert.deepStrictEqual(userData, ['Alice (Online)', '1'])

    // Update multiple fields atomically
    await redisClient?.mset(
      '{user1001}lastlogin',
      Date.now().toString(),
      '{user1001}score',
      '0',
    )

    // Increment score by points
    await redisClient?.incrby('{user1001}score', 150)
    const score = await redisClient?.get('{user1001}score')
    assert.strictEqual(score, '150')

    // Check total data length
    const nameLen = await redisClient?.strlen('{user1001}name')
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
        () => directClient.call('INCRBY', stringKey, 'abc'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.call('INCRBY', stringKey, '01'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.call('DECRBY', stringKey, '-01'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.call('INCRBYFLOAT', stringKey, 'abc'),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => directClient.call('SETRANGE', stringKey, '-1', 'x'),
        errorWithMessage('ERR offset is out of range'),
      )
      await assert.rejects(
        () => directClient.call('SETEX', `${tag}:setex`, '0', 'value'),
        errorWithMessage("ERR invalid expire time in 'setex' command"),
      )
      await assert.rejects(
        () =>
          directClient.call(
            'SETEX',
            `${tag}:setex-leading-zero`,
            '01',
            'value',
          ),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.call('PSETEX', `${tag}:psetex`, '0', 'value'),
        errorWithMessage("ERR invalid expire time in 'psetex' command"),
      )
      await assert.rejects(
        () => directClient.call('GETEX', stringKey, 'EX', '0'),
        errorWithMessage("ERR invalid expire time in 'getex' command"),
      )
      await assert.rejects(
        () => directClient.call('GETEX', stringKey, 'EX', '10', 'PX', '10'),
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
