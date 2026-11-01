import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

const testRunner = new TestRunner()

// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN] (#206), through
// node-redis' lcs / lcsLen / lcsIdx / lcsIdxWithMatchLen.
describe(`LCS Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let client: RedisClusterType

  before(async () => {
    client = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function pair(): Promise<{ a: string; b: string }> {
    const tag = `{${randomKey()}}`
    const a = `${tag}:a`
    const b = `${tag}:b`
    await client.set(a, 'ohmytext')
    await client.set(b, 'mynewtext')
    return { a, b }
  }

  test('LCS answers the subsequence, its length and its ranges', async () => {
    const { a, b } = await pair()
    assert.strictEqual(await client.lcs(a, b), 'mytext')
    assert.strictEqual(await client.lcsLen(a, b), 6)
    assert.deepStrictEqual(await client.lcsIdx(a, b), {
      matches: [
        [
          [4, 7],
          [5, 8],
        ],
        [
          [2, 3],
          [0, 1],
        ],
      ],
      len: 6,
    })
    assert.deepStrictEqual(
      await client.lcsIdxWithMatchLen(a, b, { MINMATCHLEN: 4 }),
      { matches: [[[4, 7], [5, 8], 4]], len: 6 },
    )
  })

  test('missing keys are empty strings; other types are refused', async () => {
    const tag = `{${randomKey()}}`
    assert.strictEqual(await client.lcs(`${tag}:x`, `${tag}:y`), '')
    assert.strictEqual(await client.lcsLen(`${tag}:x`, `${tag}:y`), 0)
    await client.rPush(`${tag}:list`, 'x')
    await assert.rejects(
      client.lcs(`${tag}:list`, `${tag}:x`),
      /The specified keys must contain string values/,
    )
  })
})
