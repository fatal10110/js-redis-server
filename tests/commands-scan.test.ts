import { describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisResult, RedisValue } from '../src'
import { createRedisSessionHarness as createSession } from './core-session-test-helpers'
import { assertBuffersEqual } from './shared-test-helpers'

describe('new scan commands', () => {
  test('implements KEYS and SCAN with MATCH and TYPE filters', async () => {
    const { session, server } = createSession()
    const db = server.getDatabase(0)

    await session.execute('set', [b('alpha'), b('1')])
    await session.execute('set', [b('beta'), b('2')])
    await session.execute('set', [b('user:1'), b('a')])
    await session.execute('set', [b('user:2'), b('b')])
    await session.execute('hset', [b('hash'), b('field'), b('value')])
    await session.execute('sadd', [b('set'), b('member')])
    await session.execute('zadd', [b('zset'), b('1'), b('member')])
    db.setString(b('expired'), b('gone'), { expiresAt: Date.now() - 1 })

    assert.deepStrictEqual(
      sortedBulkStrings(await session.execute('keys', [b('user:*')])),
      ['user:1', 'user:2'],
    )

    assert.deepStrictEqual(
      sortedBulkStrings(scanItems(await session.execute('scan', [b('0')]))),
      ['alpha', 'beta', 'hash', 'set', 'user:1', 'user:2', 'zset'],
    )
    assert.deepStrictEqual(
      sortedBulkStrings(
        scanItems(
          await session.execute('scan', [b('0'), b('MATCH'), b('user:*')]),
        ),
      ),
      ['user:1', 'user:2'],
    )
    assert.deepStrictEqual(
      sortedBulkStrings(
        scanItems(
          await session.execute('scan', [b('0'), b('TYPE'), b('hash')]),
        ),
      ),
      ['hash'],
    )
    assert.deepStrictEqual(
      scanItems(
        await session.execute('scan', [b('0'), b('TYPE'), b('nosuch')]),
      ),
      [],
    )
  })

  test('matches Redis glob pattern semantics for keys', async () => {
    const { session } = createSession()

    await session.execute('set', [b('a'), b('1')])
    await session.execute('set', [b('a*'), b('1')])
    await session.execute('set', [b('a/.'), b('1')])
    await session.execute('set', [b('a/b'), b('1')])
    await session.execute('set', [b('b'), b('1')])
    await session.execute('set', [b('c'), b('1')])
    await session.execute('set', [b('{a,b}'), b('1')])
    await session.execute('set', [b('^'), b('1')])
    await session.execute('set', [b('-'), b('1')])

    assert.deepStrictEqual(keys(await session.execute('keys', [b('a*')])), [
      'a',
      'a*',
      'a/.',
      'a/b',
    ])
    assert.deepStrictEqual(keys(await session.execute('keys', [b('a\\*')])), [
      'a*',
    ])
    assert.deepStrictEqual(keys(await session.execute('keys', [b('[ab]')])), [
      'a',
      'b',
    ])
    assert.deepStrictEqual(keys(await session.execute('keys', [b('[^a]')])), [
      '-',
      '^',
      'b',
      'c',
    ])
    assert.deepStrictEqual(keys(await session.execute('keys', [b('[!a]')])), [
      'a',
    ])
    assert.deepStrictEqual(keys(await session.execute('keys', [b('{a,b}')])), [
      '{a,b}',
    ])
    assert.deepStrictEqual(
      keys(await session.execute('keys', [b('[a\\-z]')])),
      ['-', 'a'],
    )
  })

  test('matches raw bytes without UTF-8 string decoding', async () => {
    const { session } = createSession()
    const invalidUtf8Key = Buffer.from([0xff, 0xfe, 0x2f, 0x80])
    const utf8Key = Buffer.from('café', 'utf8')
    const utf16LikeKey = Buffer.from('snow', 'utf16le')
    const utf8LiteralStarKey = Buffer.from([0xe2, 0x82, 0xac, 0x2a])
    const rawHashField = Buffer.from([0x00, 0xff, 0xfe])
    const rawHashValue = Buffer.from([0x80, 0x81])
    const rawSetMember = Buffer.from([0x61, 0x00, 0x62])
    const rawZsetMember = Buffer.from('😀', 'utf16le')

    await session.execute('set', [invalidUtf8Key, b('1')])
    await session.execute('set', [utf8Key, b('1')])
    await session.execute('set', [utf16LikeKey, b('1')])
    await session.execute('set', [utf8LiteralStarKey, b('1')])
    await session.execute('hset', [b('h'), rawHashField, rawHashValue])
    await session.execute('sadd', [b('s'), rawSetMember])
    await session.execute('zadd', [b('z'), b('1'), rawZsetMember])

    assertBuffersEqual(
      sortedBulkBuffers(
        await session.execute('keys', [Buffer.from([0xff, 0x3f, 0x2a])]),
      ),
      [invalidUtf8Key],
    )
    assertBuffersEqual(
      sortedBulkBuffers(await session.execute('keys', [b('caf?')])),
      [],
    )
    assertBuffersEqual(
      sortedBulkBuffers(await session.execute('keys', [b('caf??')])),
      [utf8Key],
    )
    assertBuffersEqual(
      sortedBulkBuffers(
        scanItems(
          await session.execute('scan', [
            b('0'),
            b('MATCH'),
            Buffer.from([0x73, 0x00, 0x2a]),
          ]),
        ),
      ),
      [utf16LikeKey],
    )
    assertBuffersEqual(
      sortedBulkBuffers(
        await session.execute('keys', [
          Buffer.from([0xe2, 0x82, 0xac, 0x5c, 0x2a]),
        ]),
      ),
      [utf8LiteralStarKey],
    )
    assertBuffersEqual(
      bulkBuffers(
        scanItems(
          await session.execute('hscan', [
            b('h'),
            b('0'),
            b('MATCH'),
            Buffer.from([0x00, 0x3f, 0x3f]),
          ]),
        ),
      ),
      [rawHashField, rawHashValue],
    )
    assertBuffersEqual(
      bulkBuffers(
        scanItems(
          await session.execute('sscan', [
            b('s'),
            b('0'),
            b('MATCH'),
            Buffer.from([0x61, 0x3f, 0x62]),
          ]),
        ),
      ),
      [rawSetMember],
    )
    assertBuffersEqual(
      bulkBuffers(
        scanItems(
          await session.execute('zscan', [
            b('z'),
            b('0'),
            b('MATCH'),
            Buffer.from([0x3d, 0xd8, 0x2a]),
          ]),
        ),
      ),
      [rawZsetMember, b('1')],
    )
  })

  test('implements HSCAN, SSCAN, and ZSCAN snapshots', async () => {
    const { session } = createSession()

    await session.execute('hset', [b('h'), b('f1'), b('v1'), b('f2'), b('v2')])
    await session.execute('sadd', [b('s'), b('a'), b('b'), b('c')])
    await session.execute('zadd', [b('z'), b('1'), b('a'), b('2'), b('b')])

    assert.deepStrictEqual(
      bulkStrings(
        scanItems(
          await session.execute('hscan', [b('h'), b('0'), b('MATCH'), b('f2')]),
        ),
      ),
      ['f2', 'v2'],
    )
    assert.deepStrictEqual(
      bulkStrings(
        scanItems(
          await session.execute('sscan', [b('s'), b('0'), b('MATCH'), b('b')]),
        ),
      ),
      ['b'],
    )
    assert.deepStrictEqual(
      bulkStrings(
        scanItems(
          await session.execute('zscan', [b('z'), b('0'), b('MATCH'), b('b')]),
        ),
      ),
      ['b', '2'],
    )
    assert.deepStrictEqual(
      scanItems(await session.execute('hscan', [b('missing'), b('0')])),
      [],
    )
    assert.deepStrictEqual(
      scanItems(await session.execute('sscan', [b('missing'), b('0')])),
      [],
    )
    assert.deepStrictEqual(
      scanItems(await session.execute('zscan', [b('missing'), b('0')])),
      [],
    )
  })

  test('matches Redis scan cursor, count, syntax, and wrong-type errors', async () => {
    const { session } = createSession()

    await session.execute('set', [b('alpha'), b('1')])
    await session.execute('hset', [b('h'), b('field'), b('value')])

    assert.deepStrictEqual(
      await session.execute('scan', []),
      RedisResult.error("wrong number of arguments for 'scan' command", 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('keys', []),
      RedisResult.error("wrong number of arguments for 'keys' command", 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('scan', [b('abc')]),
      RedisResult.error('invalid cursor', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('scan', [b('-1')]),
      RedisResult.error('invalid cursor', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('scan', [b('0'), b('COUNT'), b('abc')]),
      RedisResult.error('value is not an integer or out of range', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('scan', [b('0'), b('COUNT'), b('0')]),
      RedisResult.error('syntax error', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('hscan', [b('h'), b('0'), b('TYPE'), b('hash')]),
      RedisResult.error('syntax error', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('hscan', [b('alpha'), b('0')]),
      RedisResult.error(
        'Operation against a key holding the wrong kind of value',
        'WRONGTYPE',
      ),
    )
  })
})

function b(value: string): Buffer {
  return Buffer.from(value)
}

function scanItems(result: RedisResult): RedisValue[] {
  assert.strictEqual(result.value.kind, 'array')
  assert.strictEqual(result.value.items.length, 2)
  assert.deepStrictEqual(result.value.items[0], RedisValue.bulkString(b('0')))
  assert.strictEqual(result.value.items[1].kind, 'array')
  return result.value.items[1].items
}

function bulkBuffers(values: RedisValue[]): Buffer[] {
  return values.map(value => {
    assert.strictEqual(value.kind, 'bulk-string')
    assert.notStrictEqual(value.value, null)
    return value.value
  })
}

function bulkStrings(values: RedisValue[]): string[] {
  return values.map(value => {
    assert.strictEqual(value.kind, 'bulk-string')
    assert.notStrictEqual(value.value, null)
    return value.value.toString()
  })
}

function keys(result: RedisResult): string[] {
  return sortedBulkStrings(result)
}

function sortedBulkBuffers(values: RedisValue[] | RedisResult): Buffer[] {
  const items = values instanceof RedisResult ? values.value : values
  if (!Array.isArray(items)) {
    assert.strictEqual(items.kind, 'array')
    return bulkBuffers(items.items).sort(Buffer.compare)
  }

  return bulkBuffers(items).sort(Buffer.compare)
}

function sortedBulkStrings(values: RedisValue[] | RedisResult): string[] {
  const items = values instanceof RedisResult ? values.value : values
  if (!Array.isArray(items)) {
    assert.strictEqual(items.kind, 'array')
    return bulkStrings(items.items).sort()
  }

  return bulkStrings(items).sort()
}
