import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

const testRunner = new TestRunner()

// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN] (#206), through
// ioredis' typed method. The IDX reply reaches ioredis as the RESP2 flat array.
describe(`LCS Integration (${testRunner.getBackendName()})`, () => {
  let client: Cluster | undefined

  before(async () => {
    client = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function pair(): Promise<{ a: string; b: string }> {
    const tag = `{${randomKey()}}`
    const a = `${tag}:a`
    const b = `${tag}:b`
    await client!.set(a, 'ohmytext')
    await client!.set(b, 'mynewtext')
    return { a, b }
  }

  test('LCS answers the subsequence, its length and its ranges', async () => {
    const { a, b } = await pair()
    assert.strictEqual(await client!.lcs(a, b), 'mytext')
    assert.strictEqual(await client!.lcs(a, b, 'LEN'), 6)
    assert.deepStrictEqual(await client!.lcs(a, b, 'IDX'), [
      'matches',
      [
        [
          [4, 7],
          [5, 8],
        ],
        [
          [2, 3],
          [0, 1],
        ],
      ],
      'len',
      6,
    ])
    assert.deepStrictEqual(
      await client!.lcs(a, b, 'IDX', 'MINMATCHLEN', 4, 'WITHMATCHLEN'),
      ['matches', [[[4, 7], [5, 8], 4]], 'len', 6],
    )
  })

  test('missing keys are empty strings; other types are refused', async () => {
    const tag = `{${randomKey()}}`
    assert.strictEqual(await client!.lcs(`${tag}:x`, `${tag}:y`), '')
    assert.strictEqual(await client!.lcs(`${tag}:x`, `${tag}:y`, 'LEN'), 0)
    await client!.rpush(`${tag}:list`, 'x')
    await assert.rejects(
      client!.lcs(`${tag}:list`, `${tag}:x`),
      /^ReplyError: ERR The specified keys must contain string values$/,
    )
  })
})
