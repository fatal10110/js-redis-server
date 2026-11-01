import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

const testRunner = new TestRunner()

// DUMP / RESTORE through ioredis' typed methods (#202): the payload comes back
// as a Buffer (dumpBuffer) and RESTORE takes it with its options.
describe(`DUMP / RESTORE Integration (${testRunner.getBackendName()})`, () => {
  let client: Cluster | undefined

  before(async () => {
    client = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('round-trips every type', async () => {
    const tag = `{${randomKey()}}`
    await client!.set(`${tag}:s`, 'value')
    await client!.rpush(`${tag}:l`, 'a', 'b', '3')
    await client!.sadd(`${tag}:set`, '1', '2', 'x')
    await client!.hset(`${tag}:h`, { f: 'v', g: '2' })
    await client!.zadd(`${tag}:z`, 1, 'a', 2.5, 'b')
    await client!.xadd(`${tag}:x`, '1-1', 'f', 'v')

    for (const name of ['s', 'l', 'set', 'h', 'z', 'x']) {
      const payload = await client!.dumpBuffer(`${tag}:${name}`)
      assert.ok(Buffer.isBuffer(payload))
      assert.strictEqual(
        await client!.restore(`${tag}:${name}:copy`, 0, payload),
        'OK',
      )
    }
    assert.strictEqual(await client!.get(`${tag}:s:copy`), 'value')
    assert.deepStrictEqual(await client!.lrange(`${tag}:l:copy`, 0, -1), [
      'a',
      'b',
      '3',
    ])
    assert.deepStrictEqual((await client!.smembers(`${tag}:set:copy`)).sort(), [
      '1',
      '2',
      'x',
    ])
    assert.deepStrictEqual(await client!.hgetall(`${tag}:h:copy`), {
      f: 'v',
      g: '2',
    })
    assert.deepStrictEqual(
      await client!.zrange(`${tag}:z:copy`, 0, -1, 'WITHSCORES'),
      ['a', '1', 'b', '2.5'],
    )
    assert.deepStrictEqual(await client!.xrange(`${tag}:x:copy`, '-', '+'), [
      ['1-1', ['f', 'v']],
    ])
  })

  test('REPLACE, ABSTTL and BUSYKEY', async () => {
    const tag = `{${randomKey()}}`
    await client!.set(`${tag}:src`, 'v')
    const payload = await client!.dumpBuffer(`${tag}:src`)
    assert.strictEqual(await client!.dumpBuffer(`${tag}:missing`), null)

    await assert.rejects(client!.restore(`${tag}:src`, 0, payload), {
      message: 'BUSYKEY Target key name already exists.',
    })
    assert.strictEqual(
      await client!.restore(
        `${tag}:src`,
        Date.now() + 60000,
        payload,
        'REPLACE',
        'ABSTTL',
      ),
      'OK',
    )
    const pttl = await client!.pttl(`${tag}:src`)
    assert.ok(pttl > 30000 && pttl <= 60000, String(pttl))
  })
})
