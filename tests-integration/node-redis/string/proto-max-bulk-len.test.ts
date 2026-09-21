import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

/** Redis' default `proto-max-bulk-len`: 512 MB. */
const DEFAULT_PROTO_MAX_BULK_LEN = 536870912
const EXCEEDS_MAX_SIZE =
  'ERR string exceeds maximum allowed size (proto-max-bulk-len)'

/**
 * Lowering `proto-max-bulk-len` mutates server-wide state, so those tests only
 * run against the mock — flipping the limit on a shared real server would leak
 * into every other suite running against it. The real-Redis behaviour they pin
 * was ground-truthed by hand against redis-server 7.2.1 and 8.0.6.
 */
const mockOnly =
  testRunner.backend === 'real'
    ? { skip: 'mutates server-wide proto-max-bulk-len; mock backend only' }
    : {}

// CONFIG GET is a flat array on RESP2 and a map (object) on RESP3 — node-redis
// negotiates RESP3, so normalise both into a Map keyed by lower-cased name.
function configToMap(reply: unknown): Map<string, string> {
  const map = new Map<string, string>()
  if (Array.isArray(reply)) {
    for (let i = 0; i < reply.length; i += 2) {
      map.set(String(reply[i]).toLowerCase(), String(reply[i + 1]))
    }
    return map
  }
  for (const [key, value] of Object.entries(reply as Record<string, unknown>)) {
    map.set(key.toLowerCase(), String(value))
  }
  return map
}

describe(`proto-max-bulk-len enforcement (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType
  let standaloneClient: RedisClientType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    standaloneClient = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('SETRANGE rejects an offset+length beyond proto-max-bulk-len and creates no key', async () => {
    const key = `setrange:${randomKey()}`

    await assert.rejects(
      () => redisClient.setRange(key, DEFAULT_PROTO_MAX_BULK_LEN - 1, 'xx'),
      errorWithMessage(EXCEEDS_MAX_SIZE),
    )

    assert.strictEqual(await redisClient.exists(key), 0)
  })

  test('SETRANGE rejects an int64 offset far beyond the limit with the size error', async () => {
    const key = `setrange:${randomKey()}`

    // node-redis types the offset as a number, which cannot represent
    // 2^63-1 exactly — send the raw token instead.
    await assert.rejects(
      () =>
        standaloneClient.sendCommand([
          'SETRANGE',
          key,
          '9223372036854775807',
          'xx',
        ]),
      errorWithMessage(EXCEEDS_MAX_SIZE),
    )

    assert.strictEqual(await standaloneClient.exists(key), 0)
  })

  test('SETRANGE leaves an existing value untouched when the result would be too large', async () => {
    const key = `setrange:${randomKey()}`
    await redisClient.set(key, 'hello')

    await assert.rejects(
      () => redisClient.setRange(key, DEFAULT_PROTO_MAX_BULK_LEN - 1, 'xx'),
      errorWithMessage(EXCEEDS_MAX_SIZE),
    )

    assert.strictEqual(await redisClient.get(key), 'hello')
  })

  test('SETRANGE with an empty value skips the limit check and creates no key', async () => {
    const key = `setrange:${randomKey()}`

    assert.strictEqual(
      await redisClient.setRange(key, DEFAULT_PROTO_MAX_BULK_LEN - 1, ''),
      0,
    )
    assert.strictEqual(await redisClient.exists(key), 0)
  })

  test('SETRANGE rejects a negative offset before checking the limit', async () => {
    const key = `setrange:${randomKey()}`

    await assert.rejects(
      () => redisClient.setRange(key, -1, 'xx'),
      errorWithMessage('ERR offset is out of range'),
    )
  })

  test('SETRANGE rejects an offset outside the int64 range', async () => {
    const key = `setrange:${randomKey()}`

    await assert.rejects(
      () =>
        standaloneClient.sendCommand([
          'SETRANGE',
          key,
          '99999999999999999999',
          'xx',
        ]),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
  })

  test('SETRANGE reports WRONGTYPE before the size check', async () => {
    const key = `setrange:${randomKey()}`
    await redisClient.lPush(key, 'item')

    await assert.rejects(
      () => redisClient.setRange(key, DEFAULT_PROTO_MAX_BULK_LEN - 1, 'xx'),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
  })

  test('APPEND on a missing key is not size-checked', async () => {
    const key = `append:${randomKey()}`

    assert.strictEqual(await redisClient.append(key, 'hello'), 5)
    assert.strictEqual(await redisClient.get(key), 'hello')
  })

  test('GETRANGE is never size-checked, however large the requested range', async () => {
    const key = `getrange:${randomKey()}`
    await standaloneClient.set(key, 'hello')

    assert.strictEqual(
      await standaloneClient.getRange(key, 0, DEFAULT_PROTO_MAX_BULK_LEN),
      'hello',
    )
    assert.strictEqual(
      await standaloneClient.sendCommand([
        'GETRANGE',
        key,
        '0',
        '9223372036854775807',
      ]),
      'hello',
    )
    await assert.rejects(
      () =>
        standaloneClient.sendCommand([
          'GETRANGE',
          key,
          '0',
          '99999999999999999999',
        ]),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
  })

  test('CONFIG GET reports the default proto-max-bulk-len', async () => {
    const config = configToMap(
      await standaloneClient.configGet('proto-max-bulk-len'),
    )

    assert.strictEqual(
      config.get('proto-max-bulk-len'),
      String(DEFAULT_PROTO_MAX_BULK_LEN),
    )
  })

  test('CONFIG SET rejects a proto-max-bulk-len below the 1MB minimum', async () => {
    await assert.rejects(
      () => standaloneClient.configSet('proto-max-bulk-len', '100'),
      errorWithMessage(
        "ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be between 1048576 and 9223372036854775807 inclusive",
      ),
    )

    const config = configToMap(
      await standaloneClient.configGet('proto-max-bulk-len'),
    )
    assert.strictEqual(
      config.get('proto-max-bulk-len'),
      String(DEFAULT_PROTO_MAX_BULK_LEN),
    )
  })

  test('CONFIG SET rejects a proto-max-bulk-len that is not a memory value', async () => {
    await assert.rejects(
      () => standaloneClient.configSet('proto-max-bulk-len', 'abc'),
      errorWithMessage(
        "ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a memory value",
      ),
    )
    await assert.rejects(
      () => standaloneClient.configSet('proto-max-bulk-len', '1.5mb'),
      errorWithMessage(
        "ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a memory value",
      ),
    )
  })

  test(
    'CONFIG SET accepts memory-unit suffixes for proto-max-bulk-len',
    mockOnly,
    async () => {
      try {
        assert.strictEqual(
          await standaloneClient.configSet('proto-max-bulk-len', '1mb'),
          'OK',
        )
        const config = configToMap(
          await standaloneClient.configGet('proto-max-bulk-len'),
        )
        assert.strictEqual(config.get('proto-max-bulk-len'), '1048576')
      } finally {
        await standaloneClient.configSet(
          'proto-max-bulk-len',
          String(DEFAULT_PROTO_MAX_BULK_LEN),
        )
      }
    },
  )

  test(
    'SETRANGE and APPEND honour a lowered proto-max-bulk-len',
    mockOnly,
    async () => {
      const limit = 1048576
      const key = `limit:${randomKey()}`

      try {
        await standaloneClient.configSet('proto-max-bulk-len', String(limit))

        await assert.rejects(
          () => standaloneClient.setRange(key, limit - 1, 'xx'),
          errorWithMessage(EXCEEDS_MAX_SIZE),
        )
        assert.strictEqual(await standaloneClient.exists(key), 0)

        // Exactly at the limit is still allowed.
        assert.strictEqual(
          await standaloneClient.setRange(key, limit - 2, 'xx'),
          limit,
        )

        // APPEND is checked against the current value's length.
        await assert.rejects(
          () => standaloneClient.append(key, 'y'),
          errorWithMessage(EXCEEDS_MAX_SIZE),
        )
        assert.strictEqual(await standaloneClient.strLen(key), limit)
      } finally {
        await standaloneClient.del(key)
        await standaloneClient.configSet(
          'proto-max-bulk-len',
          String(DEFAULT_PROTO_MAX_BULK_LEN),
        )
      }
    },
  )
})
