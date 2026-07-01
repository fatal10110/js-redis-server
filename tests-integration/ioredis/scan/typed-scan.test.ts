import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Scan Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('scan-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function collectHashScan(
    key: string,
    count: number,
    options: Array<string | Buffer> = [],
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
        ...options,
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

  async function collectHashScanNoValues(
    key: string,
    count: number,
    match?: string,
  ): Promise<string[]> {
    const fields: string[] = []
    await new Promise<void>((resolve, reject) => {
      const stream = redisClient!.hscanStream(key, {
        count,
        ...(match ? { match } : {}),
        noValues: true,
      })

      stream.on('data', (items: string[]) => fields.push(...items))
      stream.on('error', reject)
      stream.on('end', resolve)
    })

    return fields.sort()
  }

  async function collectSetScan(
    key: string,
    count: number,
    options: Array<string | Buffer> = [],
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
        ...options,
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
    options: Array<string | Buffer> = [],
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
        ...options,
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

  test('HSCAN NOVALUES returns only matching fields', async () => {
    const key = taggedKey('hash-novalues')

    try {
      await redisClient!.hset(key, 'hit:1', 'v1', 'hit:2', 'v2', 'miss', 'v3')

      assert.deepStrictEqual(await collectHashScanNoValues(key, 1, 'hit:*'), [
        'hit:1',
        'hit:2',
      ])
    } finally {
      await redisClient!.del(key)
    }
  })

  test('keyed scan MATCH advances across non-matching COUNT batches', async () => {
    const tag = `{scan-sparse-keyed:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const setKey = `${tag}:set`
    const zsetKey = `${tag}:zset`
    const hashArgs: string[] = []
    const setMembers: string[] = []
    const zsetArgs: string[] = []

    for (let i = 0; i < 600; i++) {
      hashArgs.push(`miss:${i}`, `value:${i}`)
      setMembers.push(`miss:${i}`)
      zsetArgs.push(i.toString(), `miss:${i}`)
    }

    hashArgs.push('hit:only', 'value:only')
    setMembers.push('hit:only')
    zsetArgs.push('601', 'hit:only')

    try {
      await redisClient!.hset(hashKey, ...hashArgs)
      await redisClient!.sadd(setKey, ...setMembers)
      await redisClient!.zadd(zsetKey, ...zsetArgs)

      const hashResult = await collectHashScan(hashKey, 1, ['MATCH', 'hit:*'])
      assert.deepStrictEqual(sortedEntries(hashResult.entries), [
        ['hit:only', 'value:only'],
      ])
      assert.ok(hashResult.iterations > hashResult.entries.size)

      const setResult = await collectSetScan(setKey, 1, ['MATCH', 'hit:*'])
      assert.deepStrictEqual(sortedValues(setResult.members), ['hit:only'])
      assert.ok(setResult.iterations > setResult.members.size)

      const zsetResult = await collectSortedSetScan(zsetKey, 1, [
        'MATCH',
        'hit:*',
      ])
      assert.deepStrictEqual(sortedEntries(zsetResult.entries), [
        ['hit:only', '601'],
      ])
      assert.ok(zsetResult.iterations > zsetResult.entries.size)
    } finally {
      await redisClient!.del(hashKey, setKey, zsetKey)
    }
  })
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
