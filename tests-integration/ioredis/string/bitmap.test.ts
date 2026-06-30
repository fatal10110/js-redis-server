import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

// Every key shares one hashtag so the whole suite runs over a single direct
// (unprefixed) connection to one slot owner — multi-key BITOP stays same-slot,
// `.call()` gives exact arg control for arity/error assertions, and we avoid
// churning a fresh connection per test against the shared real cluster.
const TAG = '{bitmap}'

describe(`Bitmap Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined
  let client: Redis

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('bitmap-integration')
    client = await connectToSlotOwner(redisClient, TAG)
  })

  after(async () => {
    client?.disconnect()
    await testRunner.cleanup()
  })

  // Per-test key namespace (unique suffix) under the shared hashtag.
  const ns = (): string => `${TAG}:${randomKey()}`

  test('SETBIT / GETBIT set, return old bit, and auto-grow', async () => {
    const key = `${ns()}:sb`

    assert.strictEqual(await client.call('SETBIT', key, '7', '1'), 0)
    assert.strictEqual(await client.call('GETBIT', key, '7'), 1)
    // SETBIT returns the previous bit value.
    assert.strictEqual(await client.call('SETBIT', key, '7', '0'), 1)
    assert.strictEqual(await client.call('GETBIT', key, '7'), 0)

    // Auto-grow: setting a high bit extends the underlying string.
    assert.strictEqual(await client.call('SETBIT', key, '100', '1'), 0)
    assert.strictEqual(await client.call('STRLEN', key), 13)

    // GETBIT past the end and on a missing key reads 0.
    assert.strictEqual(await client.call('GETBIT', key, '100000'), 0)
    assert.strictEqual(await client.call('GETBIT', `${ns()}:missing`, '5'), 0)
  })

  test('SETBIT / GETBIT argument errors match Redis', async () => {
    const key = `${ns()}:sb`

    await assert.rejects(
      () => client.call('SETBIT', key, '4294967296', '1'),
      errorWithMessage('ERR bit offset is not an integer or out of range'),
    )
    await assert.rejects(
      () => client.call('SETBIT', key, '-1', '1'),
      errorWithMessage('ERR bit offset is not an integer or out of range'),
    )
    await assert.rejects(
      () => client.call('SETBIT', key, 'abc', '1'),
      errorWithMessage('ERR bit offset is not an integer or out of range'),
    )
    await assert.rejects(
      () => client.call('SETBIT', key, '7', '2'),
      errorWithMessage('ERR bit is not an integer or out of range'),
    )
    await assert.rejects(
      () => client.call('SETBIT', key, '7', '-1'),
      errorWithMessage('ERR bit is not an integer or out of range'),
    )
    await assert.rejects(
      () => client.call('SETBIT', key),
      errorWithMessage("ERR wrong number of arguments for 'setbit' command"),
    )
    await assert.rejects(
      () => client.call('GETBIT', key),
      errorWithMessage("ERR wrong number of arguments for 'getbit' command"),
    )

    // The max valid offset is 2^32 - 1.
    assert.strictEqual(
      await client.call('SETBIT', `${ns()}:max`, '4294967295', '1'),
      0,
    )
  })

  test('BITCOUNT counts set bits with byte and bit ranges', async () => {
    const key = `${ns()}:bc`
    await client.call('SET', key, 'foobar')

    assert.strictEqual(await client.call('BITCOUNT', key), 26)
    assert.strictEqual(await client.call('BITCOUNT', key, '1', '1'), 6)
    assert.strictEqual(await client.call('BITCOUNT', key, '0', '0'), 4)
    assert.strictEqual(
      await client.call('BITCOUNT', key, '0', '-1', 'BYTE'),
      26,
    )
    assert.strictEqual(await client.call('BITCOUNT', key, '5', '30', 'BIT'), 17)
    // Modifier is case-insensitive.
    assert.strictEqual(await client.call('BITCOUNT', key, '5', '30', 'bit'), 17)

    // Negative / inverted / out-of-range byte ranges.
    assert.strictEqual(await client.call('BITCOUNT', key, '-1', '-1'), 4)
    assert.strictEqual(await client.call('BITCOUNT', key, '-100', '-1'), 26)
    assert.strictEqual(await client.call('BITCOUNT', key, '2', '1'), 0)
    assert.strictEqual(await client.call('BITCOUNT', key, '100', '200'), 0)

    // Missing key counts zero.
    assert.strictEqual(await client.call('BITCOUNT', `${ns()}:missing`), 0)
  })

  test('BITCOUNT argument errors match Redis', async () => {
    const key = `${ns()}:bc`
    await client.call('SET', key, 'foobar')

    // A lone start index (no end) is a syntax error.
    await assert.rejects(
      () => client.call('BITCOUNT', key, '0'),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => client.call('BITCOUNT', key, '0', '0', 'NOPE'),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => client.call('BITCOUNT'),
      errorWithMessage("ERR wrong number of arguments for 'bitcount' command"),
    )
  })

  test('BITPOS finds the first set/clear bit', async () => {
    // Pattern with bits 12..23 set (0x00 0x0f 0xff).
    const pat = `${ns()}:pat`
    for (let i = 12; i <= 23; i++) {
      await client.call('SETBIT', pat, String(i), '1')
    }
    assert.strictEqual(await client.call('BITPOS', pat, '1'), 12)
    assert.strictEqual(await client.call('BITPOS', pat, '1', '2'), 16)
    assert.strictEqual(
      await client.call('BITPOS', pat, '1', '0', '-1', 'BIT'),
      12,
    )

    // All-ones value: searching for a clear bit.
    const ones = `${ns()}:ones`
    for (let i = 0; i <= 23; i++) {
      await client.call('SETBIT', ones, String(i), '1')
    }
    // No explicit range: returns the first bit past the end of the string.
    assert.strictEqual(await client.call('BITPOS', ones, '0'), 24)
    // With an explicit range: no clear bit found -> -1.
    assert.strictEqual(await client.call('BITPOS', ones, '0', '0', '-1'), -1)
    assert.strictEqual(await client.call('BITPOS', ones, '1'), 0)

    // Missing key: 0 when searching for a clear bit, -1 for a set bit.
    assert.strictEqual(await client.call('BITPOS', `${ns()}:missing`, '0'), 0)
    assert.strictEqual(await client.call('BITPOS', `${ns()}:missing`, '1'), -1)
  })

  test('BITPOS argument errors match Redis', async () => {
    const key = `${ns()}:bp`
    await client.call('SET', key, 'foobar')

    await assert.rejects(
      () => client.call('BITPOS', key, '2'),
      errorWithMessage('ERR The bit argument must be 1 or 0.'),
    )
    await assert.rejects(
      () => client.call('BITPOS', key),
      errorWithMessage("ERR wrong number of arguments for 'bitpos' command"),
    )
  })

  test('BITOP performs bitwise ops and stores the result', async () => {
    const tag = ns()
    const a = `${tag}:a`
    const b = `${tag}:b`
    await client.call('SET', a, 'abc') // 0x61 0x62 0x63
    await client.call('SET', b, 'abd') // 0x61 0x62 0x64

    assert.strictEqual(await client.call('BITOP', 'AND', `${tag}:and`, a, b), 3)
    assert.deepStrictEqual(
      await client.callBuffer('GET', `${tag}:and`),
      Buffer.from([0x61, 0x62, 0x60]),
    )

    assert.strictEqual(await client.call('BITOP', 'OR', `${tag}:or`, a, b), 3)
    assert.deepStrictEqual(
      await client.callBuffer('GET', `${tag}:or`),
      Buffer.from([0x61, 0x62, 0x67]),
    )

    assert.strictEqual(await client.call('BITOP', 'XOR', `${tag}:xor`, a, b), 3)
    assert.deepStrictEqual(
      await client.callBuffer('GET', `${tag}:xor`),
      Buffer.from([0x00, 0x00, 0x07]),
    )

    assert.strictEqual(await client.call('BITOP', 'NOT', `${tag}:not`, a), 3)
    assert.deepStrictEqual(
      await client.callBuffer('GET', `${tag}:not`),
      Buffer.from([0x9e, 0x9d, 0x9c]),
    )
  })

  test('BITOP zero-pads shorter operands and deletes empty results', async () => {
    const tag = ns()
    const long = `${tag}:long`
    const short = `${tag}:short`
    await client.call('SET', long, 'abc') // 3 bytes
    await client.call('SET', short, 'ab') // 2 bytes

    // Result length is the longest operand; the short one is zero-padded.
    assert.strictEqual(
      await client.call('BITOP', 'AND', `${tag}:dl`, long, short),
      3,
    )
    assert.deepStrictEqual(
      await client.callBuffer('GET', `${tag}:dl`),
      Buffer.from([0x61, 0x62, 0x00]),
    )

    // All-missing sources produce an empty result and delete the destination.
    const dest = `${tag}:dest`
    await client.call('SET', dest, 'preexisting')
    assert.strictEqual(
      await client.call('BITOP', 'AND', dest, `${tag}:m1`, `${tag}:m2`),
      0,
    )
    assert.strictEqual(await client.call('EXISTS', dest), 0)
  })

  test('BITOP argument errors match Redis', async () => {
    const tag = ns()
    const a = `${tag}:a`
    const b = `${tag}:b`
    await client.call('SET', a, 'abc')
    await client.call('SET', b, 'abd')

    await assert.rejects(
      () => client.call('BITOP', 'NOT', `${tag}:d`, a, b),
      errorWithMessage(
        'ERR BITOP NOT must be called with a single source key.',
      ),
    )
    await assert.rejects(
      () => client.call('BITOP', 'NOPE', `${tag}:d`, a),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => client.call('BITOP', 'AND', `${tag}:d`),
      errorWithMessage("ERR wrong number of arguments for 'bitop' command"),
    )
  })

  test('BITFIELD GET/SET/INCRBY with overflow modes', async () => {
    const tag = ns()
    const key = `${tag}:bf`

    // SET returns the previous value; GET reads it back.
    assert.deepStrictEqual(
      await client.call(
        'BITFIELD',
        key,
        'SET',
        'u8',
        '0',
        '255',
        'GET',
        'u8',
        '0',
      ),
      [0, 255],
    )
    // INCRBY wraps by default (255 + 10 = 9 mod 256).
    assert.deepStrictEqual(
      await client.call('BITFIELD', key, 'INCRBY', 'u8', '0', '10'),
      [9],
    )

    // OVERFLOW SAT saturates at the type maximum.
    assert.deepStrictEqual(
      await client.call(
        'BITFIELD',
        `${tag}:sat`,
        'SET',
        'u8',
        '0',
        '250',
        'OVERFLOW',
        'SAT',
        'INCRBY',
        'u8',
        '0',
        '100',
      ),
      [0, 255],
    )

    // OVERFLOW FAIL returns nil and leaves the value untouched.
    assert.deepStrictEqual(
      await client.call(
        'BITFIELD',
        `${tag}:fail`,
        'SET',
        'u8',
        '0',
        '250',
        'OVERFLOW',
        'FAIL',
        'INCRBY',
        'u8',
        '0',
        '100',
      ),
      [0, null],
    )

    // Signed SAT clamps at the type minimum.
    assert.deepStrictEqual(
      await client.call(
        'BITFIELD',
        `${tag}:smin`,
        'SET',
        'i8',
        '0',
        '-128',
        'OVERFLOW',
        'SAT',
        'INCRBY',
        'i8',
        '0',
        '-10',
      ),
      [0, -128],
    )

    // `#N` offset notation addresses the Nth field of the given width.
    assert.deepStrictEqual(
      await client.call(
        'BITFIELD',
        `${tag}:hash`,
        'SET',
        'i8',
        '#1',
        '-1',
        'GET',
        'i8',
        '8',
      ),
      [0, -1],
    )

    // GET on a missing key / past the end reads 0.
    assert.deepStrictEqual(
      await client.call('BITFIELD', `${tag}:miss`, 'GET', 'u8', '0'),
      [0],
    )
    // An empty operation list yields an empty array.
    assert.deepStrictEqual(await client.call('BITFIELD', key), [])
  })

  test('BITFIELD argument errors match Redis', async () => {
    const key = `${ns()}:bf`

    await assert.rejects(
      () => client.call('BITFIELD', key, 'GET', 'u64', '0'),
      errorWithMessage(
        'ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.',
      ),
    )
    for (const badType of ['i0', 'x8', 'i65', 'u64']) {
      await assert.rejects(
        () => client.call('BITFIELD', key, 'GET', badType, '0'),
        errorWithMessage(
          'ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.',
        ),
      )
    }
    await assert.rejects(
      () => client.call('BITFIELD', key, 'OVERFLOW', 'NOPE', 'GET', 'u8', '0'),
      errorWithMessage('ERR Invalid OVERFLOW type specified'),
    )
    await assert.rejects(
      () => client.call('BITFIELD', key, 'SET', 'u8', '0', 'abc'),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
    await assert.rejects(
      () => client.call('BITFIELD', key, 'GET', 'u8', 'abc'),
      errorWithMessage('ERR bit offset is not an integer or out of range'),
    )
    await assert.rejects(
      () => client.call('BITFIELD', key, 'GET', 'u8'),
      errorWithMessage('ERR syntax error'),
    )
  })

  test('BITFIELD_RO supports GET only', async () => {
    const key = `${ns()}:bfro`
    await client.call('BITFIELD', key, 'SET', 'i64', '0', '-100')

    assert.deepStrictEqual(
      await client.call('BITFIELD_RO', key, 'GET', 'i64', '0'),
      [-100],
    )
    await assert.rejects(
      () => client.call('BITFIELD_RO', key, 'SET', 'u8', '0', '1'),
      errorWithMessage('ERR BITFIELD_RO only supports the GET subcommand'),
    )
  })

  test('bitmap commands reject keys holding the wrong type', async () => {
    const tag = ns()
    const key = `${tag}:list`
    await client.call('RPUSH', key, 'x')

    const wrongType = errorWithMessage(
      'WRONGTYPE Operation against a key holding the wrong kind of value',
    )
    await assert.rejects(() => client.call('SETBIT', key, '0', '1'), wrongType)
    await assert.rejects(() => client.call('GETBIT', key, '0'), wrongType)
    await assert.rejects(() => client.call('BITCOUNT', key), wrongType)
    await assert.rejects(() => client.call('BITPOS', key, '1'), wrongType)
    await assert.rejects(
      () => client.call('BITFIELD', key, 'GET', 'u8', '0'),
      wrongType,
    )
    await assert.rejects(
      () => client.call('BITOP', 'AND', `${tag}:d`, key),
      wrongType,
    )
  })
})
