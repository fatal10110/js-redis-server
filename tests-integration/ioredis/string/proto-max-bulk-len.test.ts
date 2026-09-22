import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
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
 * was ground-truthed by hand against redis-server 7.2.1 and 8.0.6:
 *
 *   CONFIG SET proto-max-bulk-len 1048576
 *   SETRANGE k 1048575 xx  -> ERR string exceeds maximum allowed size (...)
 *   SETRANGE k 1048574 xx  -> 1048576
 *   APPEND   k y           -> ERR string exceeds maximum allowed size (...)
 */
const mockOnly =
  testRunner.backend === 'real'
    ? { skip: 'mutates server-wide proto-max-bulk-len; mock backend only' }
    : {}

describe(`proto-max-bulk-len enforcement (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined
  let standaloneClient: Redis | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('proto-max-bulk-len:')
    standaloneClient = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function readProtoMaxBulkLen(): Promise<string> {
    const reply = (await standaloneClient!.config(
      'GET',
      'proto-max-bulk-len',
    )) as string[]
    return reply[1]
  }

  test('SETRANGE rejects an offset+length beyond proto-max-bulk-len and creates no key', async () => {
    const key = `setrange:${randomKey()}`

    await assert.rejects(
      () => redisClient!.setrange(key, DEFAULT_PROTO_MAX_BULK_LEN - 1, 'xx'),
      errorWithMessage(EXCEEDS_MAX_SIZE),
    )

    assert.strictEqual(await redisClient!.exists(key), 0)
  })

  test('SETRANGE rejects an int64 offset far beyond the limit with the size error', async () => {
    const key = `setrange:${randomKey()}`

    await assert.rejects(
      () => redisClient!.setrange(key, '9223372036854775807', 'xx'),
      errorWithMessage(EXCEEDS_MAX_SIZE),
    )

    assert.strictEqual(await redisClient!.exists(key), 0)
  })

  test('SETRANGE leaves an existing value untouched when the result would be too large', async () => {
    const key = `setrange:${randomKey()}`
    await redisClient!.set(key, 'hello')

    await assert.rejects(
      () => redisClient!.setrange(key, DEFAULT_PROTO_MAX_BULK_LEN - 1, 'xx'),
      errorWithMessage(EXCEEDS_MAX_SIZE),
    )

    assert.strictEqual(await redisClient!.get(key), 'hello')
  })

  test('SETRANGE with an empty value skips the limit check and creates no key', async () => {
    const key = `setrange:${randomKey()}`

    assert.strictEqual(
      await redisClient!.setrange(key, DEFAULT_PROTO_MAX_BULK_LEN - 1, ''),
      0,
    )
    assert.strictEqual(await redisClient!.exists(key), 0)
  })

  test('SETRANGE rejects a negative offset before checking the limit', async () => {
    const key = `setrange:${randomKey()}`

    await assert.rejects(
      () => redisClient!.setrange(key, -1, 'xx'),
      errorWithMessage('ERR offset is out of range'),
    )
  })

  test('SETRANGE rejects an offset outside the int64 range', async () => {
    const key = `setrange:${randomKey()}`

    await assert.rejects(
      () => redisClient!.setrange(key, '99999999999999999999', 'xx'),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
  })

  test('SETRANGE reports WRONGTYPE before the size check', async () => {
    const key = `setrange:${randomKey()}`
    await redisClient!.lpush(key, 'item')

    await assert.rejects(
      () => redisClient!.setrange(key, DEFAULT_PROTO_MAX_BULK_LEN - 1, 'xx'),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
  })

  test('APPEND on a missing key is not size-checked', async () => {
    const key = `append:${randomKey()}`

    assert.strictEqual(await redisClient!.append(key, 'hello'), 5)
    assert.strictEqual(await redisClient!.get(key), 'hello')
  })

  test('GETRANGE is never size-checked, however large the requested range', async () => {
    const key = `getrange:${randomKey()}`
    await redisClient!.set(key, 'hello')

    assert.strictEqual(
      await redisClient!.getrange(key, 0, DEFAULT_PROTO_MAX_BULK_LEN),
      'hello',
    )
    assert.strictEqual(
      await redisClient!.getrange(key, 0, '9223372036854775807'),
      'hello',
    )
    await assert.rejects(
      () => redisClient!.getrange(key, 0, '99999999999999999999'),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
  })

  // Redis clamps a resolved-negative end up to 0, so an `end` that underflows
  // past the start of the value still yields the first byte. Widening the
  // accepted range to int64 makes the far-negative cases reachable instead of a
  // parse error, so they are pinned here too.
  test('GETRANGE clamps a resolved-negative end up to 0', async () => {
    const key = `getrange:${randomKey()}`
    await redisClient!.set(key, 'hello')

    for (const [start, end] of [
      [0, -5],
      [0, -6],
      [0, -100],
      [-100, -6],
      [-100, -100],
    ] as const) {
      assert.strictEqual(
        await redisClient!.getrange(key, start, end),
        'h',
        `GETRANGE ${start} ${end}`,
      )
    }

    assert.strictEqual(
      await redisClient!.getrange(key, 0, '-9223372036854775808'),
      'h',
    )
    // SUBSTR is the same implementation under a deprecated name.
    assert.strictEqual(await redisClient!.substr(key, 0, -100), 'h')

    // A start past the clamped end is still empty.
    assert.strictEqual(await redisClient!.getrange(key, 2, -100), '')
  })

  test(
    'CONFIG GET reports the default proto-max-bulk-len',
    mockOnly,
    async () => {
      const reply = (await standaloneClient!.config(
        'GET',
        'proto-max-bulk-len',
      )) as string[]

      assert.deepStrictEqual(reply, [
        'proto-max-bulk-len',
        String(DEFAULT_PROTO_MAX_BULK_LEN),
      ])
    },
  )

  test('CONFIG SET rejects a proto-max-bulk-len below the 1MB minimum', async () => {
    const before = await readProtoMaxBulkLen()

    await assert.rejects(
      () => standaloneClient!.config('SET', 'proto-max-bulk-len', '100'),
      errorWithMessage(
        "ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be between 1048576 and 9223372036854775807 inclusive",
      ),
    )

    // A rejected CONFIG SET must leave the live value alone. Compared against
    // what it was rather than the default, so a sibling suite sharing this
    // standalone cannot make it flap.
    assert.strictEqual(await readProtoMaxBulkLen(), before)
  })

  test('CONFIG SET rejects a proto-max-bulk-len that is not a memory value', async () => {
    await assert.rejects(
      () => standaloneClient!.config('SET', 'proto-max-bulk-len', 'abc'),
      errorWithMessage(
        "ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a memory value",
      ),
    )
    await assert.rejects(
      () => standaloneClient!.config('SET', 'proto-max-bulk-len', '1.5mb'),
      errorWithMessage(
        "ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a memory value",
      ),
    )
  })

  // A prototype key as a memory-unit suffix (`constructor`) is covered in
  // tests-integration/raw-tcp/proto-max-bulk-len.test.ts, for the same reason
  // as the allocation ceiling: when it regresses the server hangs up, and a
  // client-driven assertion then hangs on ioredis' retry instead of failing.
  // Measured — this test wedged the runner before it was moved.

  // Redis' memtoull reads an empty string as 0, so it fails the *range* check
  // rather than the memory-value check.
  test('CONFIG SET reports an empty proto-max-bulk-len as out of range', async () => {
    await assert.rejects(
      () => standaloneClient!.config('SET', 'proto-max-bulk-len', ''),
      errorWithMessage(
        "ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be between 1048576 and 9223372036854775807 inclusive",
      ),
    )
  })

  test(
    'CONFIG SET accepts every Redis memory-unit suffix',
    mockOnly,
    async () => {
      // The bare forms are decimal and the `b` forms binary, so a swapped pair
      // would otherwise go unnoticed. Every case is at or above the 1MB minimum.
      const cases: [value: string, bytes: string][] = [
        ['1048576', '1048576'],
        ['2097152b', '2097152'],
        ['2000k', '2000000'],
        ['2000kb', '2048000'],
        ['3m', '3000000'],
        ['3mb', '3145728'],
        ['1g', '1000000000'],
        ['1gb', '1073741824'],
        ['1MB', '1048576'],
      ]

      try {
        for (const [value, bytes] of cases) {
          assert.strictEqual(
            await standaloneClient!.config('SET', 'proto-max-bulk-len', value),
            'OK',
            `CONFIG SET proto-max-bulk-len ${value}`,
          )
          assert.strictEqual(
            await readProtoMaxBulkLen(),
            bytes,
            `CONFIG SET proto-max-bulk-len ${value}`,
          )
        }
      } finally {
        await standaloneClient!.config(
          'SET',
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
        await standaloneClient!.config(
          'SET',
          'proto-max-bulk-len',
          String(limit),
        )

        await assert.rejects(
          () => standaloneClient!.setrange(key, limit - 1, 'xx'),
          errorWithMessage(EXCEEDS_MAX_SIZE),
        )
        assert.strictEqual(await standaloneClient!.exists(key), 0)

        // Exactly at the limit is still allowed.
        assert.strictEqual(
          await standaloneClient!.setrange(key, limit - 2, 'xx'),
          limit,
        )

        // APPEND is checked against the current value's length.
        await assert.rejects(
          () => standaloneClient!.append(key, 'y'),
          errorWithMessage(EXCEEDS_MAX_SIZE),
        )
        assert.strictEqual(await standaloneClient!.strlen(key), limit)
      } finally {
        await standaloneClient!.del(key)
        await standaloneClient!.config(
          'SET',
          'proto-max-bulk-len',
          String(DEFAULT_PROTO_MAX_BULK_LEN),
        )
      }
    },
  )

  // The allocation ceiling behind a raised `proto-max-bulk-len` is covered in
  // tests-integration/raw-tcp/proto-max-bulk-len-allocation.test.ts. It cannot
  // live here: if the server ever drops the connection again, ioredis re-queues
  // the in-flight command across its reconnect and the assertion never settles,
  // so this suite would hang rather than fail.
})
