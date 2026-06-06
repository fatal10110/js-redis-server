import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { assertBufferSetsEqual, errorWithMessage, randomKey } from '../utils'

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

function sortedEntries(values: Map<string, string>): [string, string][] {
  return Array.from(values.entries()).sort(([left], [right]) =>
    left.localeCompare(right),
  )
}

function sortedValues(values: Set<string>): string[] {
  return Array.from(values).sort()
}
