import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe(`String Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('INCR and DECR commands', async () => {
    const incr1 = await redisClient.incr('counter')
    assert.strictEqual(incr1, 1)

    const incr2 = await redisClient.incr('counter')
    assert.strictEqual(incr2, 2)

    const decr1 = await redisClient.decr('counter')
    assert.strictEqual(decr1, 1)
  })

  test('INCRBY and DECRBY commands', async () => {
    const incr1 = await redisClient.incrBy('bycounter', 5)
    assert.strictEqual(incr1, 5)

    const incr2 = await redisClient.incrBy('bycounter', 3)
    assert.strictEqual(incr2, 8)

    const decr1 = await redisClient.decrBy('bycounter', 2)
    assert.strictEqual(decr1, 6)
  })

  test('INCR/INCRBY/DECR/DECRBY operate over the full int64 range', async () => {
    const tag = `{int64:${randomKey()}}`
    const key = `${tag}:counter`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      // INCR just below the int64 max reaches it exactly (precision > 2^53)
      await directClient.set(key, '9223372036854775806')
      await directClient.incr(key)
      assert.strictEqual(await directClient.get(key), '9223372036854775807')

      // INCR at the int64 max overflows and leaves the value untouched
      await assert.rejects(
        () => directClient.incr(key),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(await directClient.get(key), '9223372036854775807')

      // DECR at the int64 min overflows
      await directClient.set(key, '-9223372036854775808')
      await assert.rejects(
        () => directClient.decr(key),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(await directClient.get(key), '-9223372036854775808')

      // INCRBY with a large in-range amount keeps full precision
      await directClient.set(key, '1')
      await directClient.sendCommand(['INCRBY', key, '9223372036854775806'])
      assert.strictEqual(await directClient.get(key), '9223372036854775807')

      // INCRBY that would cross the int64 max overflows
      await assert.rejects(
        () => directClient.sendCommand(['INCRBY', key, '1']),
        errorWithMessage('ERR increment or decrement would overflow'),
      )

      // INCRBY/DECRBY amount outside the int64 range is rejected outright
      await assert.rejects(
        () =>
          directClient.sendCommand(['INCRBY', key, '99999999999999999999999']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          directClient.sendCommand(['DECRBY', key, '99999999999999999999999']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      // DECRBY by the int64 min cannot be negated -> dedicated overflow message
      await directClient.set(key, '0')
      await assert.rejects(
        () => directClient.sendCommand(['DECRBY', key, '-9223372036854775808']),
        errorWithMessage('ERR decrement would overflow'),
      )

      // DECRBY large in-range amount keeps full precision
      await directClient.set(key, '-1')
      await directClient.sendCommand(['DECRBY', key, '9223372036854775807'])
      assert.strictEqual(await directClient.get(key), '-9223372036854775808')

      // INCR on a value already out of int64 range is rejected
      await directClient.set(key, '99999999999999999999999')
      await assert.rejects(
        () => directClient.incr(key),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    } finally {
      await directClient.del(key)
      directClient.destroy()
    }
  })

  test('INCRBYFLOAT command', async () => {
    const incr1 = await redisClient.incrByFloat('floatcounter', 1.5)
    assert.strictEqual(incr1, '1.5')

    const incr2 = await redisClient.incrByFloat('floatcounter', 2.3)
    assert.strictEqual(incr2, '3.8')
  })

  test('INCRBYFLOAT distinguishes invalid-float from NaN/Infinity result (#56)', async () => {
    const tag = `{incrbyfloat-inf:${randomKey()}}`
    const key = `${tag}:key`
    const direct = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      // A result of +/-Infinity (an infinity increment) reports the
      // post-arithmetic error, NOT the "value is not a valid float" parse error.
      for (const increment of [
        'inf',
        '+inf',
        '-inf',
        'infinity',
        'Inf',
        'INF',
      ]) {
        await direct.set(key, '3.0')
        await assert.rejects(
          () => direct.sendCommand(['INCRBYFLOAT', key, increment]),
          errorWithMessage('ERR increment would produce NaN or Infinity'),
          `increment "${increment}" should report the NaN/Infinity error`,
        )
        // The key is left untouched on error.
        assert.strictEqual(await direct.get(key), '3.0')
      }

      // A stored infinity plus a finite increment is still infinity.
      await direct.set(key, 'inf')
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, '1']),
        errorWithMessage('ERR increment would produce NaN or Infinity'),
      )

      // inf + (-inf) = NaN — also the post-arithmetic error.
      await direct.set(key, 'inf')
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, '-inf']),
        errorWithMessage('ERR increment would produce NaN or Infinity'),
      )

      // Genuinely non-numeric / overflow-magnitude increments are parse errors.
      await direct.set(key, '1')
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, 'nan']),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, '1e5000']),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, 'abc']),
        errorWithMessage('ERR value is not a valid float'),
      )

      // Redis parses the whole token (strtold): trailing junk, leading or
      // trailing whitespace, and non-'.' separators are all invalid floats,
      // not silent prefix parses.
      for (const bad of ['3abc', '3.5x', ' 3.5', '3.5 ', '1,5', '', '0x']) {
        await direct.set(key, '1')
        await assert.rejects(
          () => direct.sendCommand(['INCRBYFLOAT', key, bad]),
          errorWithMessage('ERR value is not a valid float'),
          `increment "${bad}" should be an invalid float`,
        )
        // The key is left untouched on a parse error.
        assert.strictEqual(await direct.get(key), '1')
      }

      // A finite increment still works normally.
      await direct.set(key, '3')
      assert.strictEqual(
        await direct.sendCommand(['INCRBYFLOAT', key, '1.0e2']),
        '103',
      )

      // Arity.
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key]),
        errorWithMessage(
          "ERR wrong number of arguments for 'incrbyfloat' command",
        ),
      )
    } finally {
      await direct.del(key)
      direct.destroy()
    }
  })

  test('APPEND command', async () => {
    const append1 = await redisClient.append('appendkey', 'hello')
    assert.strictEqual(append1, 5)

    const append2 = await redisClient.append('appendkey', ' world')
    assert.strictEqual(append2, 11)

    const value = await redisClient.get('appendkey')
    assert.strictEqual(value, 'hello world')
  })

  test('STRLEN command', async () => {
    const len1 = await redisClient.strLen('nonexistent')
    assert.strictEqual(len1, 0)

    await redisClient.set('strlenkey', 'hello')
    const len2 = await redisClient.strLen('strlenkey')
    assert.strictEqual(len2, 5)
  })

  test('MGET command', async () => {
    await redisClient.set('{same}mget1', 'value1')
    await redisClient.set('{same}mget2', 'value2')

    const values = await redisClient.mGet([
      '{same}mget1',
      '{same}mget2',
      '{same}nonexistent',
    ])
    assert.deepStrictEqual(values, ['value1', 'value2', null])
  })

  test('MGET cross-slot error', async () => {
    await redisClient.set('{mget-slot-a}key', 'value1')
    await redisClient.set('{mget-slot-b}key', 'value2')

    await assert.rejects(
      () => redisClient.mGet(['{mget-slot-a}key', '{mget-slot-b}key']),
      errorWithMessage("CROSSSLOT Keys in request don't hash to the same slot"),
    )
  })

  test('MSET command', async () => {
    await redisClient.mSet([
      ['{same}mset1', 'value1'],
      ['{same}mset2', 'value2'],
      ['{same}mset3', 'value3'],
    ])

    const get1 = await redisClient.get('{same}mset1')
    const get2 = await redisClient.get('{same}mset2')
    const get3 = await redisClient.get('{same}mset3')

    assert.strictEqual(get1, 'value1')
    assert.strictEqual(get2, 'value2')
    assert.strictEqual(get3, 'value3')
  })

  test('MSETNX command', async () => {
    // All keys new
    const result1 = await redisClient.mSetNX([
      ['{same}msetnx1', 'value1'],
      ['{same}msetnx2', 'value2'],
    ])
    assert.strictEqual(result1, 1)

    // Some keys exist
    const result2 = await redisClient.mSetNX([
      ['{same}msetnx1', 'newvalue'],
      ['{same}msetnx3', 'value3'],
    ])
    assert.strictEqual(result2, 0)

    // Verify original values unchanged
    const check = await redisClient.get('{same}msetnx1')
    assert.strictEqual(check, 'value1')
  })

  test('GETSET command', async () => {
    await redisClient.set('getsetkey', 'oldvalue')

    const oldValue = await redisClient.getSet('getsetkey', 'newvalue')
    assert.strictEqual(oldValue, 'oldvalue')

    const newValue = await redisClient.get('getsetkey')
    assert.strictEqual(newValue, 'newvalue')

    // GETSET on non-existent key
    const nullValue = await redisClient.getSet('newgetsetkey', 'firstvalue')
    assert.strictEqual(nullValue, null)
  })

  test('SUBSTR aliases GETRANGE', async () => {
    const tag = `{substr:${randomKey()}}`
    const key = `${tag}:key`
    const missingKey = `${tag}:missing`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.set(key, 'abcdef')

      assert.strictEqual(
        await directClient.sendCommand(['SUBSTR', key, '1', '3']),
        'bcd',
      )
      assert.strictEqual(
        await directClient.sendCommand(['SUBSTR', key, '-3', '-1']),
        'def',
      )
      assert.strictEqual(
        await directClient.sendCommand(['SUBSTR', missingKey, '0', '1']),
        '',
      )
    } finally {
      await directClient.del([key, missingKey])
      directClient.destroy()
    }
  })

  test('String commands workflow', async () => {
    // Create a session counter with user data
    await redisClient.set('{user1001}name', 'Alice')
    await redisClient.set('{user1001}sessions', '0')

    // Increment session count
    const sessions1 = await redisClient.incr('{user1001}sessions')
    assert.strictEqual(sessions1, 1)

    // Add login timestamp
    await redisClient.append('{user1001}name', ' (Online)')
    const nameWithStatus = await redisClient.get('{user1001}name')
    assert.strictEqual(nameWithStatus, 'Alice (Online)')

    // Get multiple user fields
    const userData = await redisClient.mGet([
      '{user1001}name',
      '{user1001}sessions',
    ])
    assert.deepStrictEqual(userData, ['Alice (Online)', '1'])

    // Update multiple fields atomically
    await redisClient.mSet([
      ['{user1001}lastlogin', Date.now().toString()],
      ['{user1001}score', '0'],
    ])

    // Increment score by points
    await redisClient.incrBy('{user1001}score', 150)
    const score = await redisClient.get('{user1001}score')
    assert.strictEqual(score, '150')

    // Check total data length
    const nameLen = await redisClient.strLen('{user1001}name')
    assert.strictEqual(nameLen, 14) // 'Alice (Online)'.length
  })

  test('basic SET and GET', async () => {
    await redisClient.set('testkey', 'testvalue')
    const value = await redisClient.get('testkey')
    assert.strictEqual(value, 'testvalue')
  })

  test('SET with EX option', async () => {
    await redisClient.set('exkey', 'exvalue', {
      expiration: { type: 'EX', value: 10 },
    })
    const value = await redisClient.get('exkey')
    assert.strictEqual(value, 'exvalue')

    const ttl = await redisClient.ttl('exkey')
    assert.ok(ttl > 0 && ttl <= 10)
  })

  test('SET with PX option', async () => {
    await redisClient.set('pxkey', 'pxvalue', {
      expiration: { type: 'PX', value: 5000 },
    })
    const value = await redisClient.get('pxkey')
    assert.strictEqual(value, 'pxvalue')

    const ttl = await redisClient.pTTL('pxkey')
    assert.ok(ttl > 0 && ttl <= 5000)
  })

  test('SET with NX option - key does not exist', async () => {
    const result = await redisClient.set('nxkey1', 'nxvalue', {
      condition: 'NX',
    })
    assert.strictEqual(result, 'OK')

    const value = await redisClient.get('nxkey1')
    assert.strictEqual(value, 'nxvalue')
  })

  test('SET with NX option - key exists', async () => {
    await redisClient.set('nxkey2', 'existing')
    const result = await redisClient.set('nxkey2', 'newvalue', {
      condition: 'NX',
    })
    assert.strictEqual(result, null)

    const value = await redisClient.get('nxkey2')
    assert.strictEqual(value, 'existing')
  })

  test('SET with XX option - key exists', async () => {
    await redisClient.set('xxkey1', 'existing')
    const result = await redisClient.set('xxkey1', 'newvalue', {
      condition: 'XX',
    })
    assert.strictEqual(result, 'OK')

    const value = await redisClient.get('xxkey1')
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with XX option - key does not exist', async () => {
    const result = await redisClient.set('xxkey2', 'newvalue', {
      condition: 'XX',
    })
    assert.strictEqual(result, null)

    const value = await redisClient.get('xxkey2')
    assert.strictEqual(value, null)
  })

  test('SET with GET option', async () => {
    await redisClient.set('getkey', 'oldvalue')

    const result = await redisClient.set('getkey', 'newvalue', { GET: true })
    assert.strictEqual(result, 'oldvalue')

    const value = await redisClient.get('getkey')
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with multiple options', async () => {
    await redisClient.set('multikey', 'existing')

    // XX with EX
    const result = await redisClient.set('multikey', 'newvalue', {
      condition: 'XX',
      expiration: { type: 'EX', value: 5 },
    })
    assert.strictEqual(result, 'OK')

    const value = await redisClient.get('multikey')
    assert.strictEqual(value, 'newvalue')

    const ttl = await redisClient.ttl('multikey')
    assert.ok(ttl > 0 && ttl <= 5)
  })

  test('SET KEEPTTL preserves the existing expiration', async () => {
    const key = `{set-keepttl:${randomKey()}}:key`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.set(key, 'ttl', {
        expiration: { type: 'PX', value: 5000 },
      })
      const originalTtl = await directClient.pTTL(key)
      assert.ok(originalTtl > 0 && originalTtl <= 5000)

      assert.strictEqual(
        await directClient.set(key, 'kept', { expiration: 'KEEPTTL' }),
        'OK',
      )
      assert.strictEqual(await directClient.get(key), 'kept')

      const keptTtl = await directClient.pTTL(key)
      assert.ok(keptTtl > 0 && keptTtl <= originalTtl)
    } finally {
      await directClient.del(key)
      directClient.destroy()
    }
  })

  test('SET and GET wrong-type and syntax errors match Redis', async () => {
    const tag = `{set-errors:${randomKey()}}`
    const listKey = `${tag}:list`
    const stringKey = `${tag}:string`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, listKey)

    try {
      await directClient.lPush(listKey, 'value')

      await assert.rejects(
        () => directClient.get(listKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient.sendCommand(['SET', listKey, 'value', 'GET']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient.sendCommand(['SET', stringKey, 'value', 'NX', 'XX']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['SET', stringKey, 'value', 'EX', '0']),
        errorWithMessage("ERR invalid expire time in 'set' command"),
      )
    } finally {
      await directClient.del([listKey, stringKey])
      directClient.destroy()
    }
  })

  test('String numeric and expiration errors match Redis', async () => {
    const tag = `{string-errors:${randomKey()}}`
    const stringKey = `${tag}:string`
    const leadingZeroKey = `${tag}:leading-zero`
    const negativeLeadingZeroKey = `${tag}:negative-leading-zero`
    const zeroKey = `${tag}:zero`
    const negativeZeroKey = `${tag}:negative-zero`
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      stringKey,
    )

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
        () => directClient.sendCommand(['INCRBY', stringKey, 'abc']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['INCRBY', stringKey, '01']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['DECRBY', stringKey, '-01']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['INCRBYFLOAT', stringKey, 'abc']),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['SETRANGE', stringKey, '-1', 'x']),
        errorWithMessage('ERR offset is out of range'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['SETEX', `${tag}:setex`, '0', 'value']),
        errorWithMessage("ERR invalid expire time in 'setex' command"),
      )
      await assert.rejects(
        () =>
          directClient.sendCommand([
            'SETEX',
            `${tag}:setex-leading-zero`,
            '01',
            'value',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          directClient.sendCommand(['PSETEX', `${tag}:psetex`, '0', 'value']),
        errorWithMessage("ERR invalid expire time in 'psetex' command"),
      )
      await assert.rejects(
        () => directClient.sendCommand(['GETEX', stringKey, 'EX', '0']),
        errorWithMessage("ERR invalid expire time in 'getex' command"),
      )
      await assert.rejects(
        () =>
          directClient.sendCommand([
            'GETEX',
            stringKey,
            'EX',
            '10',
            'PX',
            '10',
          ]),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await directClient.del([
        stringKey,
        leadingZeroKey,
        negativeLeadingZeroKey,
        zeroKey,
        negativeZeroKey,
        `${tag}:setex`,
        `${tag}:setex-leading-zero`,
        `${tag}:psetex`,
      ])
      directClient.destroy()
    }
  })

  test('MGET returns null for keys holding non-string values', async () => {
    const tag = `{mget-types:${randomKey()}}`
    const stringKey = `${tag}:string`
    const listKey = `${tag}:list`
    const missingKey = `${tag}:missing`

    await redisClient.set(stringKey, 'A')
    await redisClient.lPush(listKey, 'B')

    assert.deepStrictEqual(
      await redisClient.mGet([stringKey, listKey, missingKey]),
      ['A', null, null],
    )
  })
})
