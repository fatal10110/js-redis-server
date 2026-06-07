import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import {
  assertBufferSetsEqual,
  connectToSlotOwner,
  errorWithMessage,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe(`Scan Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('scan-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HSCAN iterates with COUNT until cursor 0', async () => {
    const key = taggedKey('hash')
    const expected = new Map<string, string>()
    const args: string[] = []

    for (let i = 0; i < 600; i++) {
      const field = `field:${i}`
      const value = `value:${i}`
      expected.set(field, value)
      args.push(field, value)
    }

    await redisClient?.hset(key, ...args)

    const result = await collectHashScan(key, 25)
    assert.ok(result.iterations > 1)
    assert.deepStrictEqual(
      sortedEntries(result.entries),
      sortedEntries(expected),
    )
  })

  test('KEYS and top-level SCAN support MATCH and TYPE filters', async () => {
    const tag = `{scan-top:${randomKey()}}`
    const keys = [
      `${tag}:alpha`,
      `${tag}:beta`,
      `${tag}:user:1`,
      `${tag}:user:2`,
      `${tag}:hash`,
      `${tag}:set`,
      `${tag}:zset`,
    ]
    const directClient = await connectToSlotOwner(redisClient!, keys[0])

    try {
      await directClient.set(keys[0], '1')
      await directClient.set(keys[1], '2')
      await directClient.set(keys[2], 'a')
      await directClient.set(keys[3], 'b')
      await directClient.hset(keys[4], 'field', 'value')
      await directClient.sadd(keys[5], 'member')
      await directClient.zadd(keys[6], 1, 'member')

      assert.deepStrictEqual(
        stripTag(await directClient.keys(`${tag}:user:*`), tag),
        ['user:1', 'user:2'],
      )

      assert.deepStrictEqual(
        stripTag(
          await collectTopLevelScan(directClient, ['MATCH', `${tag}:user:*`]),
          tag,
        ),
        ['user:1', 'user:2'],
      )

      assert.deepStrictEqual(
        stripTag(
          await collectTopLevelScan(directClient, [
            'MATCH',
            `${tag}:*`,
            'TYPE',
            'hash',
          ]),
          tag,
        ),
        ['hash'],
      )

      assert.deepStrictEqual(
        await collectTopLevelScan(directClient, [
          'MATCH',
          `${tag}:*`,
          'TYPE',
          'nosuch',
        ]),
        [],
      )
    } finally {
      await directClient.del(...keys)
      directClient.disconnect()
    }
  })

  test('KEYS matches Redis glob pattern semantics', async () => {
    const tag = `{scan-glob:${randomKey()}}`
    const names = ['a', 'a*', 'a/.', 'a/b', 'b', 'c', '{a,b}', '^', '-']
    const keys = names.map(name => `${tag}:${name}`)
    const directClient = await connectToSlotOwner(redisClient!, keys[0])

    try {
      for (const key of keys) {
        await directClient.set(key, '1')
      }

      assert.deepStrictEqual(
        stripTag(await directClient.keys(`${tag}:a*`), tag),
        ['a', 'a*', 'a/.', 'a/b'],
      )
      assert.deepStrictEqual(
        stripTag(await directClient.keys(`${tag}:a\\*`), tag),
        ['a*'],
      )
      assert.deepStrictEqual(
        stripTag(await directClient.keys(`${tag}:[ab]`), tag),
        ['a', 'b'],
      )
      assert.deepStrictEqual(
        stripTag(await directClient.keys(`${tag}:[^a]`), tag),
        ['-', '^', 'b', 'c'],
      )
      assert.deepStrictEqual(
        stripTag(await directClient.keys(`${tag}:[!a]`), tag),
        ['a'],
      )
      assert.deepStrictEqual(
        stripTag(await directClient.keys(`${tag}:{a,b}`), tag),
        ['{a,b}'],
      )
      assert.deepStrictEqual(
        stripTag(await directClient.keys(`${tag}:[a\\-z]`), tag),
        ['-', 'a'],
      )
      assert.deepStrictEqual(
        stripTag(await directClient.keys(`${tag}:[a-]`), tag),
        ['^', 'a'],
      )
      assert.deepStrictEqual(
        stripTag(
          await collectTopLevelScan(directClient, ['MATCH', `${tag}:[a-]`]),
          tag,
        ),
        ['^', 'a'],
      )
    } finally {
      await directClient.del(...keys)
      directClient.disconnect()
    }
  })

  test('KEYS and top-level SCAN MATCH treat patterns as raw bytes', async () => {
    const tag = `{scan-raw:${randomKey()}}:`
    const prefix = Buffer.from(tag)
    const invalidUtf8Key = bufferKey(prefix, [0xff, 0xfe, 0x2f, 0x80])
    const utf8Key = Buffer.concat([prefix, Buffer.from('café', 'utf8')])
    const utf16LikeKey = Buffer.concat([prefix, Buffer.from('snow', 'utf16le')])
    const utf8LiteralStarKey = bufferKey(prefix, [0xe2, 0x82, 0xac, 0x2a])
    const directClient = await connectToSlotOwner(redisClient!, invalidUtf8Key)

    try {
      await directClient.set(invalidUtf8Key, '1')
      await directClient.set(utf8Key, '1')
      await directClient.set(utf16LikeKey, '1')
      await directClient.set(utf8LiteralStarKey, '1')

      const invalidUtf8Keys = (await directClient.callBuffer(
        'KEYS',
        bufferKey(prefix, [0xff, 0x3f, 0x2a]),
      )) as Buffer[]
      assertBufferSetsEqual(invalidUtf8Keys, [invalidUtf8Key])

      const singleByteCafeKeys = (await directClient.callBuffer(
        'KEYS',
        Buffer.concat([prefix, Buffer.from('caf?')]),
      )) as Buffer[]
      assert.deepStrictEqual(singleByteCafeKeys, [])

      const twoByteCafeKeys = (await directClient.callBuffer(
        'KEYS',
        Buffer.concat([prefix, Buffer.from('caf??')]),
      )) as Buffer[]
      assertBufferSetsEqual(twoByteCafeKeys, [utf8Key])

      assertBufferSetsEqual(
        await collectTopLevelScanBuffers(directClient, [
          'MATCH',
          bufferKey(prefix, [0x73, 0x00, 0x2a]),
        ]),
        [utf16LikeKey],
      )

      const literalStarKeys = (await directClient.callBuffer(
        'KEYS',
        bufferKey(prefix, [0xe2, 0x82, 0xac, 0x5c, 0x2a]),
      )) as Buffer[]
      assertBufferSetsEqual(literalStarKeys, [utf8LiteralStarKey])
    } finally {
      await directClient.del(
        invalidUtf8Key,
        utf8Key,
        utf16LikeKey,
        utf8LiteralStarKey,
      )
      directClient.disconnect()
    }
  })

  test('SSCAN iterates with COUNT until cursor 0', async () => {
    const key = taggedKey('set')
    const expected = new Set<string>()
    const members: string[] = []

    for (let i = 0; i < 300; i++) {
      const member = `member:${i}`
      expected.add(member)
      members.push(member)
    }

    await redisClient?.sadd(key, ...members)

    const result = await collectSetScan(key, 20)
    assert.ok(result.iterations > 1)
    assert.deepStrictEqual(sortedValues(result.members), sortedValues(expected))
  })

  test('ZSCAN iterates with COUNT until cursor 0', async () => {
    const key = taggedKey('zset')
    const expected = new Map<string, string>()
    const args: string[] = []

    for (let i = 0; i < 300; i++) {
      const score = i.toString()
      const member = `member:${i}`
      expected.set(member, score)
      args.push(score, member)
    }

    await redisClient?.zadd(key, ...args)

    const result = await collectSortedSetScan(key, 20)
    assert.ok(result.iterations > 1)
    assert.deepStrictEqual(
      sortedEntries(result.entries),
      sortedEntries(expected),
    )
  })

  test('scan MATCH treats patterns as raw bytes', async () => {
    const hashKey = taggedKey('raw-hash')
    const setKey = taggedKey('raw-set')
    const zsetKey = taggedKey('raw-zset')
    const rawHashField = Buffer.from([0x00, 0xff, 0xfe])
    const rawHashValue = Buffer.from([0x80, 0x81])
    const rawSetMember = Buffer.from([0x61, 0x00, 0x62])
    const rawZsetMember = Buffer.from('😀', 'utf16le')

    await redisClient?.call('hset', hashKey, rawHashField, rawHashValue)
    await redisClient?.call('sadd', setKey, rawSetMember)
    await redisClient?.call('zadd', zsetKey, '1', rawZsetMember)

    const hscan = (await redisClient?.callBuffer(
      'hscan',
      hashKey,
      '0',
      'MATCH',
      Buffer.from([0x00, 0x3f, 0x3f]),
    )) as [Buffer, Buffer[]]
    assert.strictEqual(hscan[0].toString(), '0')
    assertBufferSetsEqual(hscan[1], [rawHashField, rawHashValue])

    const sscan = (await redisClient?.callBuffer(
      'sscan',
      setKey,
      '0',
      'MATCH',
      Buffer.from([0x61, 0x3f, 0x62]),
    )) as [Buffer, Buffer[]]
    assert.strictEqual(sscan[0].toString(), '0')
    assertBufferSetsEqual(sscan[1], [rawSetMember])

    const zscan = (await redisClient?.callBuffer(
      'zscan',
      zsetKey,
      '0',
      'MATCH',
      Buffer.from([0x3d, 0xd8, 0x2a]),
    )) as [Buffer, Buffer[]]
    assert.strictEqual(zscan[0].toString(), '0')
    assertBufferSetsEqual(zscan[1], [rawZsetMember, Buffer.from('1')])
  })

  test('scan COUNT errors match Redis', async () => {
    const key = taggedKey('errors')
    await redisClient?.hset(key, 'field', 'value')

    await assert.rejects(
      () => redisClient?.call('SCAN'),
      errorWithMessage("ERR wrong number of arguments for 'scan' command"),
    )
    await assert.rejects(
      () => redisClient?.call('KEYS'),
      errorWithMessage("ERR wrong number of arguments for 'keys' command"),
    )
    await assert.rejects(
      () => redisClient?.scan('abc'),
      errorWithMessage('ERR invalid cursor'),
    )
    assert.deepStrictEqual(await redisClient?.scan('-1', 'MATCH', 'missing'), [
      '0',
      [],
    ])
    await assert.rejects(
      () => redisClient?.scan('0', 'COUNT', 'abc'),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
    await assert.rejects(
      () => redisClient?.scan('0', 'COUNT', '0'),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      () => redisClient?.hscan(key, '0', 'COUNT', 'abc'),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
    await assert.rejects(
      () => redisClient?.hscan(key, '0', 'COUNT', '0'),
      errorWithMessage('ERR syntax error'),
    )
  })

  async function collectHashScan(
    key: string,
    count: number,
  ): Promise<{ entries: Map<string, string>; iterations: number }> {
    const entries = new Map<string, string>()
    let cursor = '0'
    let iterations = 0

    do {
      const [nextCursor, items] = await redisClient!.hscan(
        key,
        cursor,
        'COUNT',
        count,
      )
      assert.strictEqual(items.length % 2, 0)

      for (let i = 0; i < items.length; i += 2) {
        entries.set(items[i], items[i + 1])
      }

      cursor = nextCursor
      iterations++
      assert.ok(iterations < 1000)
    } while (cursor !== '0')

    return { entries, iterations }
  }

  async function collectSetScan(
    key: string,
    count: number,
  ): Promise<{ members: Set<string>; iterations: number }> {
    const members = new Set<string>()
    let cursor = '0'
    let iterations = 0

    do {
      const [nextCursor, items] = await redisClient!.sscan(
        key,
        cursor,
        'COUNT',
        count,
      )

      for (const item of items) {
        members.add(item)
      }

      cursor = nextCursor
      iterations++
      assert.ok(iterations < 1000)
    } while (cursor !== '0')

    return { members, iterations }
  }

  async function collectSortedSetScan(
    key: string,
    count: number,
  ): Promise<{ entries: Map<string, string>; iterations: number }> {
    const entries = new Map<string, string>()
    let cursor = '0'
    let iterations = 0

    do {
      const [nextCursor, items] = await redisClient!.zscan(
        key,
        cursor,
        'COUNT',
        count,
      )
      assert.strictEqual(items.length % 2, 0)

      for (let i = 0; i < items.length; i += 2) {
        entries.set(items[i], items[i + 1])
      }

      cursor = nextCursor
      iterations++
      assert.ok(iterations < 1000)
    } while (cursor !== '0')

    return { entries, iterations }
  }
})

function taggedKey(name: string): string {
  return `{scan:${randomKey()}}:${name}`
}

function bufferKey(prefix: Buffer, bytes: number[]): Buffer {
  return Buffer.concat([prefix, Buffer.from(bytes)])
}

async function collectTopLevelScan(
  client: Redis,
  options: Array<string | Buffer>,
): Promise<string[]> {
  const values: string[] = []
  let cursor = '0'
  let iterations = 0

  do {
    const [nextCursor, items] = (await client.scan(cursor, ...options)) as [
      string,
      string[],
    ]
    values.push(...items)
    cursor = nextCursor
    iterations++
    assert.ok(iterations < 1000)
  } while (cursor !== '0')

  return values.sort()
}

async function collectTopLevelScanBuffers(
  client: Redis,
  options: Array<string | Buffer>,
): Promise<Buffer[]> {
  const values: Buffer[] = []
  let cursor = Buffer.from('0')
  let iterations = 0

  do {
    const [nextCursor, items] = (await client.callBuffer(
      'SCAN',
      cursor,
      ...options,
    )) as [Buffer, Buffer[]]
    values.push(...items)
    cursor = nextCursor
    iterations++
    assert.ok(iterations < 1000)
  } while (cursor.toString() !== '0')

  return values
}

function stripTag(keys: string[], tag: string): string[] {
  return keys.map(key => key.slice(tag.length + 1)).sort()
}

function sortedEntries(values: Map<string, string>): [string, string][] {
  return Array.from(values.entries()).sort(([left], [right]) =>
    left.localeCompare(right),
  )
}

function sortedValues(values: Set<string>): string[] {
  return Array.from(values).sort()
}
