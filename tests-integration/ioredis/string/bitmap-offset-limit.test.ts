import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

/**
 * `SETBIT` / `GETBIT` / `BITFIELD` cap a bit offset at the live
 * `proto-max-bulk-len` rather than at a fixed 2^32 (#415): Redis rejects an
 * offset whose *byte* is at or past the limit — `(offset >> 3) >=
 * server.proto_max_bulk_len`, bitops.c `getBitOffsetFromArgument`. At the 512MB
 * default that is exactly "offset < 2^32", which is why the two only diverge
 * once `proto-max-bulk-len` became a live setting.
 *
 * Ground-truthed against redis-server 6.2.24, 7.2.16 and 8.0.6 — identical on
 * all three, so no compatibility gate applies. With the limit at 1048576:
 *
 *   SETBIT k 8388608 1        -> ERR bit offset is not an integer or out of range
 *   SETBIT k 8388607 1        -> 0
 *   BITFIELD k GET u8 8388608 -> ERR bit offset is not an integer or out of range
 *
 * Everything here goes through `.call()` for the same reason as the sibling
 * `bitmap.test.ts`: these are argument/error assertions that need byte-exact
 * control of the tokens on the wire — `#`-prefixed offsets, and an invalid bit
 * value sent *alongside* an out-of-range offset to pin which error wins.
 *
 * The suite runs against a standalone server so that `CONFIG SET` and the bit
 * command it affects always reach the same node.
 */
const DEFAULT_PROTO_MAX_BULK_LEN = 536870912
/** The lowest offset the default limit refuses: 512MB * 8. */
const OVER_DEFAULT_OFFSET = DEFAULT_PROTO_MAX_BULK_LEN * 8
const LOWERED = 1048576
/** The lowest offset a 1MB limit refuses. */
const OVER_LOWERED_OFFSET = LOWERED * 8
const BIT_OFFSET_ERROR = 'ERR bit offset is not an integer or out of range'

/**
 * Lowering `proto-max-bulk-len` mutates server-wide state, so those tests only
 * run against the mock — flipping the limit on the shared real standalone would
 * leak into every other suite running against it. Their real behaviour is the
 * ground truth quoted above.
 */
const mockOnly =
  testRunner.backend === 'real'
    ? { skip: 'mutates server-wide proto-max-bulk-len; mock backend only' }
    : {}

describe(`Bitmap offset ceiling vs proto-max-bulk-len (${testRunner.getBackendName()})`, () => {
  let client: Redis

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  const ns = (): string => `bitlimit:${randomKey()}`

  async function setLimit(value: number): Promise<void> {
    assert.strictEqual(
      await client.config('SET', 'proto-max-bulk-len', String(value)),
      'OK',
    )
  }

  // GETBIT never allocates, so it can probe the ceiling at the 512MB default
  // without the 512MB buffer a SETBIT at the same offset would create.
  test('at the default limit the ceiling is 2^32 bits', async () => {
    const key = ns()

    assert.strictEqual(await client.call('GETBIT', key, '4294967295'), 0)
    await assert.rejects(
      () => client.call('GETBIT', key, String(OVER_DEFAULT_OFFSET)),
      errorWithMessage(BIT_OFFSET_ERROR),
    )
    await assert.rejects(
      () => client.call('SETBIT', key, String(OVER_DEFAULT_OFFSET), '1'),
      errorWithMessage(BIT_OFFSET_ERROR),
    )
  })

  test('SETBIT reports an out-of-range offset before an invalid bit value', async () => {
    const key = ns()

    await assert.rejects(
      () => client.call('SETBIT', key, String(OVER_DEFAULT_OFFSET), '2'),
      errorWithMessage(BIT_OFFSET_ERROR),
    )
    // ...and a valid offset still surfaces the bit-value error.
    await assert.rejects(
      () => client.call('SETBIT', key, '5', '2'),
      errorWithMessage('ERR bit is not an integer or out of range'),
    )
  })

  test('BITFIELD uses the same ceiling at the default limit', async () => {
    const key = ns()

    // 4294967288 >> 3 == 536870911, the last addressable byte.
    assert.deepStrictEqual(
      await client.call('BITFIELD', key, 'GET', 'u8', '4294967288'),
      [0],
    )
    // A wide type starting on that same byte is still accepted: the ceiling
    // tests only the byte the offset *starts* in, with no allowance for the
    // field's width. A `(bits-1)/8` term would refuse this; Redis does not.
    assert.deepStrictEqual(
      await client.call('BITFIELD', key, 'GET', 'i64', '4294967288'),
      [0],
    )
    await assert.rejects(
      () =>
        client.call('BITFIELD', key, 'GET', 'i64', String(OVER_DEFAULT_OFFSET)),
      errorWithMessage(BIT_OFFSET_ERROR),
    )
    await assert.rejects(
      () =>
        client.call('BITFIELD', key, 'GET', 'u8', String(OVER_DEFAULT_OFFSET)),
      errorWithMessage(BIT_OFFSET_ERROR),
    )
    await assert.rejects(
      () =>
        client.call(
          'BITFIELD_RO',
          key,
          'GET',
          'u8',
          String(OVER_DEFAULT_OFFSET),
        ),
      errorWithMessage(BIT_OFFSET_ERROR),
    )
  })

  test(
    'lowering proto-max-bulk-len lowers the SETBIT/GETBIT ceiling',
    mockOnly,
    async () => {
      const key = ns()
      await setLimit(LOWERED)

      try {
        await assert.rejects(
          () => client.call('SETBIT', key, String(OVER_LOWERED_OFFSET), '1'),
          errorWithMessage(BIT_OFFSET_ERROR),
        )
        await assert.rejects(
          () => client.call('GETBIT', key, String(OVER_LOWERED_OFFSET)),
          errorWithMessage(BIT_OFFSET_ERROR),
        )
        assert.strictEqual(await client.exists(key), 0)

        // One bit below the new ceiling still works, and grows the string to
        // exactly the limit.
        assert.strictEqual(
          await client.call(
            'SETBIT',
            key,
            String(OVER_LOWERED_OFFSET - 1),
            '1',
          ),
          0,
        )
        assert.strictEqual(await client.strlen(key), LOWERED)
        await client.del(key)
      } finally {
        await setLimit(DEFAULT_PROTO_MAX_BULK_LEN)
      }
    },
  )

  test(
    'lowering proto-max-bulk-len lowers the BITFIELD ceiling',
    mockOnly,
    async () => {
      const key = ns()
      await setLimit(LOWERED)

      try {
        await assert.rejects(
          () =>
            client.call(
              'BITFIELD',
              key,
              'GET',
              'u8',
              String(OVER_LOWERED_OFFSET),
            ),
          errorWithMessage(BIT_OFFSET_ERROR),
        )
        await assert.rejects(
          () =>
            client.call(
              'BITFIELD_RO',
              key,
              'GET',
              'u8',
              String(OVER_LOWERED_OFFSET),
            ),
          errorWithMessage(BIT_OFFSET_ERROR),
        )

        // The `#<index>` form multiplies by the type width before the check.
        await assert.rejects(
          () => client.call('BITFIELD', key, 'GET', 'u8', `#${LOWERED}`),
          errorWithMessage(BIT_OFFSET_ERROR),
        )
        assert.deepStrictEqual(
          await client.call('BITFIELD', key, 'GET', 'u8', `#${LOWERED - 1}`),
          [0],
        )

        // An out-of-range offset anywhere in the op list rejects the whole
        // command — the earlier SET must not have been applied.
        await assert.rejects(
          () =>
            client.call(
              'BITFIELD',
              key,
              'SET',
              'u8',
              '0',
              '1',
              'GET',
              'u8',
              String(OVER_LOWERED_OFFSET),
            ),
          errorWithMessage(BIT_OFFSET_ERROR),
        )
        assert.strictEqual(await client.exists(key), 0)
      } finally {
        await setLimit(DEFAULT_PROTO_MAX_BULK_LEN)
      }
    },
  )

  test('the ceiling follows the setting back up', mockOnly, async () => {
    const key = ns()
    await setLimit(LOWERED)

    try {
      await assert.rejects(
        () => client.call('GETBIT', key, String(OVER_LOWERED_OFFSET)),
        errorWithMessage(BIT_OFFSET_ERROR),
      )

      await setLimit(DEFAULT_PROTO_MAX_BULK_LEN)

      assert.strictEqual(
        await client.call('GETBIT', key, String(OVER_LOWERED_OFFSET)),
        0,
      )
      assert.deepStrictEqual(
        await client.call(
          'BITFIELD',
          key,
          'GET',
          'u8',
          String(OVER_LOWERED_OFFSET),
        ),
        [0],
      )
    } finally {
      await setLimit(DEFAULT_PROTO_MAX_BULK_LEN)
    }
  })

  /**
   * For a *write*, the ceiling is the lower of `proto-max-bulk-len` and the
   * mock's materialisation cap, so raising the setting past 512MB does not hand
   * a single SETBIT or BITFIELD SET/INCRBY an unbounded allocation inside the
   * test process. Reads are not capped, because they never allocate.
   *
   * Mock-only, and the write half is a deliberate divergence: with the limit at
   * 600MB real Redis admits these offsets. It is the mock's version of what
   * MAX_MATERIALISABLE_LENGTH already does for APPEND/SETRANGE.
   *
   * Nothing here allocates, capped or not. Each write probe carries an invalid
   * value, so the answer is decided purely by which check runs first: with the
   * cap it is the bit-offset error, without it the offset passes and the value
   * is rejected — ground-truthed on redis 6.2.24, 7.2.16 and 8.0.6 at
   * `proto-max-bulk-len 629145600`:
   *
   *   SETBIT k 4294967296 2                  -> ERR bit is not an integer ...
   *   BITFIELD k SET u8 4294967296 notanint  -> ERR value is not an integer ...
   *   GETBIT k 4294967296                    -> 0
   *   BITFIELD k GET u8 4294967296           -> [0]
   */
  test(
    'raising the limit past 512MB raises the read ceiling but not the write ceiling',
    mockOnly,
    async () => {
      const key = ns()
      await setLimit(629145600) // 600MB, comfortably past the 512MB cap

      try {
        await assert.rejects(
          () => client.call('SETBIT', key, String(OVER_DEFAULT_OFFSET), '2'),
          errorWithMessage(BIT_OFFSET_ERROR),
        )
        for (const op of ['SET', 'INCRBY']) {
          await assert.rejects(
            () =>
              client.call(
                'BITFIELD',
                key,
                op,
                'u8',
                String(OVER_DEFAULT_OFFSET),
                'notanint',
              ),
            errorWithMessage(BIT_OFFSET_ERROR),
            `BITFIELD ${op}`,
          )
        }

        // Reads follow the raised setting exactly as real Redis does.
        assert.strictEqual(
          await client.call('GETBIT', key, String(OVER_DEFAULT_OFFSET)),
          0,
        )
        assert.deepStrictEqual(
          await client.call(
            'BITFIELD',
            key,
            'GET',
            'u8',
            String(OVER_DEFAULT_OFFSET),
          ),
          [0],
        )
        assert.deepStrictEqual(
          await client.call(
            'BITFIELD_RO',
            key,
            'GET',
            'u8',
            String(OVER_DEFAULT_OFFSET),
          ),
          [0],
        )
        assert.strictEqual(await client.exists(key), 0)
      } finally {
        await setLimit(DEFAULT_PROTO_MAX_BULK_LEN)
      }
    },
  )
})
