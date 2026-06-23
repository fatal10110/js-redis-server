import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { flushNodeRedisCluster, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Scan Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function collectHashScan(
    key: string,
    count: number,
    match?: string,
  ): Promise<{ entries: Map<string, string>; iterations: number }> {
    const entries = new Map<string, string>()
    let cursor = '0'
    let iterations = 0

    do {
      const reply = await redisClient.hScan(key, cursor, {
        COUNT: count,
        ...(match ? { MATCH: match } : {}),
      })

      for (const { field, value } of reply.entries) {
        entries.set(field, value)
      }

      cursor = String(reply.cursor)
      iterations++
      assert.ok(iterations < 1000)
    } while (cursor !== '0')

    return { entries, iterations }
  }

  async function collectSetScan(
    key: string,
    count: number,
    match?: string,
  ): Promise<{ members: Set<string>; iterations: number }> {
    const members = new Set<string>()
    let cursor = '0'
    let iterations = 0

    do {
      const reply = await redisClient.sScan(key, cursor, {
        COUNT: count,
        ...(match ? { MATCH: match } : {}),
      })

      for (const member of reply.members) {
        members.add(member)
      }

      cursor = String(reply.cursor)
      iterations++
      assert.ok(iterations < 1000)
    } while (cursor !== '0')

    return { members, iterations }
  }

  async function collectSortedSetScan(
    key: string,
    count: number,
    match?: string,
  ): Promise<{ entries: Map<string, string>; iterations: number }> {
    const entries = new Map<string, string>()
    let cursor = '0'
    let iterations = 0

    do {
      const reply = await redisClient.zScan(key, cursor, {
        COUNT: count,
        ...(match ? { MATCH: match } : {}),
      })

      for (const { value, score } of reply.members) {
        entries.set(value, String(score))
      }

      cursor = String(reply.cursor)
      iterations++
      assert.ok(iterations < 1000)
    } while (cursor !== '0')

    return { entries, iterations }
  }

  test('HSCAN iterates with COUNT until cursor 0', async () => {
    const key = taggedKey('hash')
    const expected = new Map<string, string>()
    const fields: Record<string, string> = {}

    for (let i = 0; i < 600; i++) {
      const field = `field:${i}`
      const value = `value:${i}`
      expected.set(field, value)
      fields[field] = value
    }

    await redisClient.hSet(key, fields)

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

    await redisClient.sAdd(key, members)

    const result = await collectSetScan(key, 20)
    assert.ok(result.iterations > 1)
    assert.deepStrictEqual(sortedValues(result.members), sortedValues(expected))
  })

  test('ZSCAN iterates with COUNT until cursor 0', async () => {
    const key = taggedKey('zset')
    const expected = new Map<string, string>()
    const args: Array<{ score: number; value: string }> = []

    for (let i = 0; i < 300; i++) {
      const score = i.toString()
      const member = `member:${i}`
      expected.set(member, score)
      args.push({ score: i, value: member })
    }

    await redisClient.zAdd(key, args)

    const result = await collectSortedSetScan(key, 20)
    assert.ok(result.iterations > 1)
    assert.deepStrictEqual(
      sortedEntries(result.entries),
      sortedEntries(expected),
    )
  })

  test('keyed scan MATCH advances across non-matching COUNT batches', async () => {
    const tag = `{scan-sparse-keyed:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const setKey = `${tag}:set`
    const zsetKey = `${tag}:zset`
    const hashFields: Record<string, string> = {}
    const setMembers: string[] = []
    const zsetArgs: Array<{ score: number; value: string }> = []

    for (let i = 0; i < 600; i++) {
      hashFields[`miss:${i}`] = `value:${i}`
      setMembers.push(`miss:${i}`)
      zsetArgs.push({ score: i, value: `miss:${i}` })
    }

    hashFields['hit:only'] = 'value:only'
    setMembers.push('hit:only')
    zsetArgs.push({ score: 601, value: 'hit:only' })

    try {
      await redisClient.hSet(hashKey, hashFields)
      await redisClient.sAdd(setKey, setMembers)
      await redisClient.zAdd(zsetKey, zsetArgs)

      const hashResult = await collectHashScan(hashKey, 1, 'hit:*')
      assert.deepStrictEqual(sortedEntries(hashResult.entries), [
        ['hit:only', 'value:only'],
      ])
      assert.ok(hashResult.iterations > hashResult.entries.size)

      const setResult = await collectSetScan(setKey, 1, 'hit:*')
      assert.deepStrictEqual(sortedValues(setResult.members), ['hit:only'])
      assert.ok(setResult.iterations > setResult.members.size)

      const zsetResult = await collectSortedSetScan(zsetKey, 1, 'hit:*')
      assert.deepStrictEqual(sortedEntries(zsetResult.entries), [
        ['hit:only', '601'],
      ])
      assert.ok(zsetResult.iterations > zsetResult.entries.size)
    } finally {
      await redisClient.del([hashKey, setKey, zsetKey])
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
